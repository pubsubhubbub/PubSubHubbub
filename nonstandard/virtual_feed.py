#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Virtual feed receiver for publishing aggregate feeds using fat pings.

This isn't part of the PubSubHubbub spec. We have no intentions of making it to
be part of the spec. This extension hook is useful for creating a virtual
feed (e.g., a firehose feed) using an aggregation of other feeds that are
fat pinged to the hub.

It's up to your code to connect your fat-ping request handler to the
'inject_virtual_feed' method (see fat_publish.py for one way to do it). That
function will parse your feed, extract the entries, and enqueue the virtual
feed update. Multiple fat pings delivered in this manner will be collated
together by their virtual feed topics into a combined payload, which is then
injected into the reference hub's event delivery pipeline. This collation is
useful because it controls how many HTTP requests will be sent to subscribers
to this virtual feed, and lets you make the tradeoff between delivery latency
and request overhead.
"""

import logging

from google.appengine.ext import db
from google.appengine.ext import webapp

import fork_join_queue

################################################################################
# Constants

VIRTUAL_FEED_QUEUE = 'virtual-feeds'

################################################################################

# Define these symbols for testing.
if 'register' not in globals():
  class Hook(object):
    pass
  def work_queue_only(func):
    return func
  sha1_hash = None


class FeedFragment(db.Model):
  """Represents a fragment of a virtual feed that will be collated.

  The Key and key_name are not used.

  Fields:
    topic: The topic of the virtual feed being collated.
    header_footer: The feed envelope.
    entries: The <entry>...</entry> text segments that were parsed from the
      source feeds, already joined together with newlines.
    format: 'rss' or 'atom'.
  """
  topic = db.TextProperty()
  header_footer = db.TextProperty()
  entries = db.TextProperty()
  format = db.TextProperty()


VIRTUAL_FEED_QUEUE = fork_join_queue.MemcacheForkJoinQueue(
    FeedFragment,
    None,
    '/work/virtual_feeds',
    VIRTUAL_FEED_QUEUE,
    batch_size=20,
    batch_period_ms=1000,
    lock_timeout_ms=1000,
    sync_timeout_ms=250,
    stall_timeout_ms=30000,
    acquire_timeout_ms=10,
    acquire_attempts=50,
    shard_count=1,
    expiration_seconds=60)  # Give up on fragments after 60 seconds.


def inject_virtual_feed(topic, format, header_footer, entries_map):
  """Injects a virtual feed update to be collated and then delievered.

  Args:
    topic: The topic URL for the virtual feed.
    format: The format of the virtual feed ('rss' or 'atom').
    header_footer: The feed envelope to use for the whole virtual feed.
    entries_map: Dictionary mapping feed entry IDs to strings containing
      full entry payloads (e.g., from <entry> to </entry> including the tags).

  Raises:
    MemcacheError if the virtual feed could not be injected.
  """
  fragment = FeedFragment(
      key=db.Key.from_path(FeedFragment.kind(), 'unused'),
      topic=topic,
      header_footer=header_footer,
      entries='\n'.join(entries_map.values()),
      format=format)

  # Update the name of the queue to include a hash of the topic URL. This
  # allows us to use a single VIRTUAL_FEED_QUEUE instance to represent a
  # different logical queue for each virtual feed topic we would like to
  # collate.
  VIRTUAL_FEED_QUEUE.name = 'fjq-%s-%s-' % (
      FeedFragment.kind(), sha1_hash(topic))
  work_index = VIRTUAL_FEED_QUEUE.next_index()
  try:
    VIRTUAL_FEED_QUEUE.put(work_index, [fragment])
  finally:
    VIRTUAL_FEED_QUEUE.add(work_index)


class CollateFeedHandler(webapp.RequestHandler):
  """Worker handler that collates virtual feed updates and enqueues them."""

  @work_queue_only
  def post(self):
    # Restore the pseudo-name of the queue so we can properly pop tasks
    # from it for the specific virtual feed this task is targetting.
    task_name = self.request.headers['X-AppEngine-TaskName']
    VIRTUAL_FEED_QUEUE.name, rest = task_name.split('--')
    VIRTUAL_FEED_QUEUE.name += '-'

    fragment_list = VIRTUAL_FEED_QUEUE.pop_request(self.request)
    if not fragment_list:
      logging.warning('Pop of virtual feed task %r found no fragments.',
                      task_name)
      return

    fragment = fragment_list[0]
    entry_payloads = [f.entries for f in fragment_list]

    def txn():
      event_to_deliver = EventToDeliver.create_event_for_topic(
          fragment.topic, fragment.format, fragment.header_footer,
          entry_payloads, set_parent=False, max_failures=1)
      db.put(event_to_deliver)
      event_to_deliver.enqueue()

    db.run_in_transaction(txn)
    logging.debug('Injected %d fragments for virtual topic %r',
                  len(fragment_list), fragment.topic)


class VirtualFeedHook(Hook):
  """Adds the CollateFeedHandler to the list of request handlers."""

  def inspect(self, args, kwargs):
    args[0].append((r'/work/virtual_feeds', CollateFeedHandler))
    return False


if 'register' in globals():
  register(modify_handlers, VirtualFeedHook())
