#!/usr/bin/env python
#
# Copyright 2008 Google Inc.
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

"""Publish-subscribe hub implementation built on Google App Engine."""

import datetime
import hashlib
import logging
import random
import urllib
import wsgiref.handlers

from google.appengine.api import datastore_types
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.runtime import apiproxy_errors

import async_apiproxy
import feed_diff
import urlfetch_async

async_proxy = async_apiproxy.AsyncAPIProxy()

################################################################################
# Config parameters

DEBUG = True

if DEBUG:
  logging.getLogger().setLevel(logging.DEBUG)

# How long a subscription will last before it must be renewed by the subscriber.
EXPIRATION_DELTA = datetime.timedelta(days=90)

# How long to hold a lock after query_and_own(), in seconds.
LEASE_PERIOD_SECONDS = 15

# How many subscribers to contact at a time when delivering events.
EVENT_SUBSCRIBER_CHUNK_SIZE = 10

################################################################################
# Helper functions

def sha1_hash(value):
  """Returns the sha1 hash of the supplied value."""
  return hashlib.sha1(value).hexdigest()


def get_hash_key_name(value):
  """Returns a valid entity key_name that's a hash of the supplied value."""
  return 'hash_' + sha1_hash(value)


def query_and_own(model_class, gql_query, lease_period,
                  work_count=1, sample_ratio=20, lock_ratio=4, **gql_bindings):
  """Query for work to do and temporarily own it.

  Args:
    model_class: The db.Model sub-class that contains the work to do.
    gql_query: String containing the GQL query that will retrieve the work
      to do in order of priority for this model_class.
    lease_period: How long in seconds that the newly retrieved worked should
      be owned before being retried later.
    work_count: Maximum number of owned items to retrieve.
    sample_ratio: How many times work_count items to query for before randomly
      selecting up to work_count of them to take ownership of. Increase this
      ratio if there are a lot of workers (and thus more contention).
    lock_ratio: How many times work_count items to try to lock at a time.
      Increase this ratio if the lease period is low and the overall work
      throughput is high.
    **gql_bindings: Any other keyword-based GQL bindings to use in the query
      for more work;

  Returns:
    If work_count is greater than 1, this function returns a list of model_class
    instances up to work_count in length if work could be retrieved. An empty
    list will be returned if there is no work to do or work could not be
    retrieved (due to collisions, etc).
    
    If work_count is 1, this function will return a single model_class if work
    could be retrieved. If there is no work left to do or work could not be
    retrieved, this function will return None.
  """
  sample_size = work_count * sample_ratio
  work_to_do = model_class.gql(gql_query, **gql_bindings).fetch(sample_size)
  if not work_to_do:
    if work_count == 1:
      return None
    else:
      return []

  # Attempt to lock more work than we actually need to do, since there likely
  # will be conflicts if the number of workers is high or the work_count is
  # high. If we've acquired more than we can use, we'll just delete the memcache
  # key and unlock the work. This is much better than an iterative solution,
  # since a single locking API call per worker reduces the locking window.
  possible_work = random.sample(work_to_do,
      min(len(work_to_do), lock_ratio * work_count))
  work_map = dict((str(w.key()), w) for w in possible_work)
  try_lock_map = dict((k, 'owned') for k in work_map)
  not_set_keys = set(memcache.add_multi(try_lock_map, time=lease_period))
  if len(not_set_keys) == len(try_lock_map):
    logging.warning(
        'Conflict; failed to acquire any locks for model %s. Tried: %s',
        model_class.kind(), not_set_keys)
  
  locked_keys = [k for k in work_map if k not in not_set_keys]
  reset_keys = locked_keys[work_count:]
  if reset_keys and not memcache.delete_multi(reset_keys):
    logging.warning('Could not reset acquired work for model %s: %s',
                    model_class.kind(), reset_keys)

  work = [work_map[k] for k in locked_keys[:work_count]]
  if work_count == 1:
    if work:
      return work[0]
    else:
      return None
  else:
    return work

################################################################################
# Models

# TODO: Change the methods of this class to return entity instances or None
# if they could not be found, instead of True/False.
class Subscription(db.Model):
  """Represents a single subscription to a topic for a callback URL."""

  STATE_NOT_VERIFIED = 'not_verified'
  STATE_VERIFIED = 'verified'
  STATE_TO_DELETE = 'to_delete'
  STATES = frozenset([
    STATE_NOT_VERIFIED,
    STATE_VERIFIED,
    STATE_TO_DELETE,
  ])

  callback = db.TextProperty(required=True)
  callback_hash = db.StringProperty(required=True)
  topic = db.TextProperty(required=True)
  topic_hash = db.StringProperty(required=True)
  created_time = db.DateTimeProperty(auto_now_add=True)
  last_modified = db.DateTimeProperty(auto_now=True)
  expiration_time = db.DateTimeProperty(required=True)
  subscription_state = db.StringProperty(default=STATE_NOT_VERIFIED,
                                         choices=STATES)

  @staticmethod
  def create_key_name(callback, topic):
    """Returns the key name for a Subscription entity.

    Args:
      callback: URL of the callback subscriber.
      topic: URL of the topic being subscribed to.

    Returns:
      String containing the key name for the corresponding Subscription.
    """
    return get_hash_key_name('%s\n%s' % (callback, topic))

  @classmethod
  def insert(cls, callback, topic):
    """Marks a callback URL as being subscribed to a topic.

    Creates a new subscription if None already exists. Forces any existing,
    pending request (i.e., async) to immediately enter the verified state.

    Args:
      callback: URL that will receive callbacks.
      topic: The topic to subscribe to.

    Returns:
      True if the subscription was newly created, False otherwise.
    """
    key_name = cls.create_key_name(callback, topic)
    def txn():
      sub_is_new = False
      sub = cls.get_by_key_name(key_name)
      if sub is None:
        sub_is_new = True
        sub = cls(key_name=key_name,
                  callback=callback,
                  callback_hash=sha1_hash(callback),
                  topic=topic,
                  topic_hash=sha1_hash(topic),
                  expiration_time=datetime.datetime.now() + EXPIRATION_DELTA)
      sub.subscription_state = cls.STATE_VERIFIED
      sub.put()
      return sub_is_new
    return db.run_in_transaction(txn)

  @classmethod
  def request_insert(cls, callback, topic):
    """Records that a callback URL needs verification before being subscribed.

    Creates a new subscription request (for asynchronous verification) if None
    already exists. Any existing subscription request will not be modified;
    for instance, if a subscription has already been verified, this method
    will do nothing.

    Args:
      callback: URL that will receive callbacks.
      topic: The topic to subscribe to.

    Returns:
      True if the subscription request was newly created, False otherwise.
    """
    key_name = cls.create_key_name(callback, topic)
    def txn():
      sub_is_new = False
      sub = cls.get_by_key_name(key_name)
      if sub is None:
        sub_is_new = True
        sub = cls(key_name=key_name,
                  callback=callback,
                  callback_hash=sha1_hash(callback),
                  topic=topic,
                  topic_hash=sha1_hash(topic),
                  expiration_time=datetime.datetime.now() + EXPIRATION_DELTA)
        sub.put()
      return sub_is_new
    return db.run_in_transaction(txn)

  @classmethod
  def remove(cls, callback, topic):
    """Causes a callback URL to no longer be subscribed to a topic.

    If the callback was not already subscribed to the topic, this method
    will do nothing. Otherwise, the subscription will immediately be removed.

    Args:
      callback: URL that will receive callbacks.
      topic: The topic to subscribe to.

    Returns:
      True if the subscription had previously existed, False otherwise.
    """
    key_name = cls.create_key_name(callback, topic)
    def txn():
      sub = cls.get_by_key_name(key_name)
      if sub is not None:
        sub.delete()
        return True
      return False
    return db.run_in_transaction(txn)

  @classmethod
  def request_remove(cls, callback, topic):
    """Records that a callback URL needs to be unsubscribed.

    Creates a new request to unsubscribe a callback URL from a topic (where
    verification should happen asynchronously). If an unsubscribe request
    has already been made, this method will do nothing.

    Args:
      callback: URL that will receive callbacks.
      topic: The topic to subscribe to.

    Returns:
      True if the unsubscribe request is new, False otherwise (i.e., a request
      for asynchronous unsubscribe was already made).
    """
    key_name = cls.create_key_name(callback, topic)
    def txn():
      sub = cls.get_by_key_name(key_name)
      if sub is not None and sub.subscription_state != cls.STATE_TO_DELETE:
        sub.subscription_state = cls.STATE_TO_DELETE
        sub.put()
        return True
      return False
    return db.run_in_transaction(txn)

  @classmethod
  def has_subscribers(cls, topic):
    """Check if a topic URL has verified subscribers.

    Args:
      topic: The topic URL to check for subscribers.

    Returns:
      True if it has verified subscribers, False otherwise.
    """
    if (cls.all().filter('topic_hash =', sha1_hash(topic))
        .filter('subscription_state = ', cls.STATE_VERIFIED).get() is not None):
      return True
    else:
      return False

  @classmethod
  def get_subscribers(cls, topic, count, starting_at_callback=None):
    """Gets the list of subscribers starting at an offset.

    Args:
      topic: The topic URL to retrieve subscribers for.
      count: How many subscribers to retrieve.
      starting_at_callback: A string containing the callback hash to offset
        to when retrieving more subscribers. The callback at the given offset
        *will* be included in the results. If None, then subscribers will
        be retrieved from the beginning.
    
    Returns:
      List of Subscription objects that were found, or an empty list if none
      were found.
    """
    query = cls.all()
    query.filter('topic_hash =', sha1_hash(topic))
    query.filter('subscription_state = ', cls.STATE_VERIFIED)
    if starting_at_callback:
      query.filter('callback_hash >=', sha1_hash(starting_at_callback))
    query.order('callback_hash')

    return query.fetch(count)
  
  @classmethod
  def get_confirm_work(cls):
    """Retrieves a Subscription to verify or remove asynchronously.

    Returns:
      A Subscription instance, or None if no work is available. The returned
      instance needs to have its status updated by confirming the subscription
      is still desired by the callback URL.
    """
    return query_and_own(cls,
        'WHERE subscription_state IN :valid_states ORDER BY last_modified ASC',
        LEASE_PERIOD_SECONDS,
        valid_states=[cls.STATE_NOT_VERIFIED, cls.STATE_TO_DELETE])
        


class FeedToFetch(db.Model):
  """A feed that has new data that needs to be pulled.

  The key name of this entity is a get_hash_key_name() hash of the topic URL, so
  multiple inserts will only ever write a single entity.
  """

  topic = db.TextProperty(required=True)
  update_time = db.DateTimeProperty(auto_now=True)

  @classmethod
  def insert(cls, topic_list):
    """Inserts a set of FeedToFetch entities for a set of topics.

    Overwrites any existing entities that are already there.

    Args:
      topic_list: List of the topic URLs of feeds that need to be fetched.
    """
    feed_list = [cls(key_name=get_hash_key_name(topic), topic=topic)
                 for topic in topic_list]
    db.put(feed_list)

  @classmethod
  def get_work(cls):
    """Retrieves a feed to fetch and owns it by acquiring a temporary lock.

    Returns:
      A FeedToFetch entity that has been owned, or None if there is currently
      no work to do. Callers should invoke delete() on the entity once the
      work has been completed.
    """
    return query_and_own(cls, 'ORDER BY update_time ASC', LEASE_PERIOD_SECONDS)


class FeedRecord(db.Model):
  """Represents the content of a feed without any entries.

  This is everything in a feed except for the entry data. That means any
  footers, top-level XML elements, namespace declarations, etc, will be
  captured in this entity. We keep these around just to have an idea of how
  many feeds there are in existence being tracked by the system. In the future
  we could use these records to track interesting statistics.

  The key name of this entity is a get_hash_key_name() of the topic URL.
  """

  topic = db.TextProperty(required=True)
  topic_hash = db.StringProperty(required=True)
  header_footer = db.TextProperty(required=True)
  last_updated = db.DateTimeProperty(auto_now=True)

  @classmethod
  def get_by_topic(cls, topic):
    """Retrieves a FeedRecord entity by its topic.

    Args:
      topic: The topic URL to retrieve the FeedRecord for.

    Returns:
      The FeedRecord for this topic, or None if it could not be found.
    """
    return cls.get_by_key_name(get_hash_key_name(topic))

  @classmethod
  def create_record(cls, topic, header_footer):
    """Creates a FeedRecord representing its current state.

    This does not insert the new entity into the Datastore. It is just returned
    so it can be submitted later as part of a batch put().

    Args:
      topic: The topic URL to update the header_footer for.
      header_footer: Contents of the feed's XML document minus the entry data.

    Returns:
      A FeedRecord instance with the supplied parameters.
    """
    return cls(key_name=get_hash_key_name(topic),
               topic=topic,
               topic_hash=sha1_hash(topic),
               header_footer=header_footer)


class FeedEntryRecord(db.Model):
  """Represents a feed entry that has been seen.

  The key name of this entity is a get_hash_key_name() hash of the combination
  of the topic URL and the entry_id.
  """

  topic = db.TextProperty(required=True)
  topic_hash = db.StringProperty(required=True)
  entry_id = db.TextProperty(required=True)
  entry_id_hash = db.StringProperty(required=True)
  entry_updated = db.StringProperty(required=True)  # ISO 8601
  entry_payload = db.TextProperty(required=True)

  @staticmethod
  def create_key_name(topic, entry_id):
    """Creates a new key name for a FeedEntryRecord entity.

    Args:
      topic: The topic URL to retrieve entries for.
      entry_id: String containing the entry_id.

    Returns:
      String containing the corresponding key name.
    """
    return get_hash_key_name('%s\n%s' % (topic, entry_id))

  @classmethod
  def get_entries_for_topic(cls, topic, entry_id_list):
    """Gets multiple FeedEntryRecord entities for a topic by their entry_ids.

    Args:
      topic: The topic URL to retrieve entries for.
      entry_id_list: Sequence of entry_ids to retrieve.

    Returns:
      List of FeedEntryRecords that were found, if any.
    """
    results = cls.get_by_key_name([cls.create_key_name(topic, entry_id)
                                   for entry_id in entry_id_list])
    # Filter out those pesky Nones.
    return [r for r in results if r]

  @classmethod
  def create_entry_for_topic(cls, topic, entry_id, updated, xml_data):
    """Creates multiple FeedEntryRecords entities for a topic.

    Does not actually insert the entities into the Datastore. This is left to
    the caller so they can do it as part of a larger batch put().

    Args:
      topic: The topic URL to insert entities for.
      entry_id: String containing the ID of the entry.
      updated: String containing the ISO 8601 timestamp of when the entry
        was last updated.
      xml_data: String containing the entry XML data. For example, with Atom
        this would be everything from <entry> to </entry> with the surrounding
        tags included.

    Returns:
      A new FeedEntryRecord that should be inserted into the Datastore.
    """
    return cls(key_name=cls.create_key_name(topic, entry_id),
               topic=topic,
               topic_hash=sha1_hash(topic),
               entry_id=entry_id,
               entry_id_hash=sha1_hash(entry_id),
               entry_updated=updated,
               entry_payload=xml_data)


class EventToDeliver(db.Model):
  """Represents a publishing event to deliver to subscribers.
  
  This model is meant to be used together with Subscription entities. When a
  feed has new published data and needs to be pushed to subscribers, one of
  these entities will be inserted. The background worker should iterate
  through all Subscription entities for this topic, sending them the event
  payload. The update() method should be used to track the progress of the
  background worker as well as any Subscription entities that failed delivery.
  
  The key_name for each of these entities is unique. It is up to the event
  injection side of the system to de-dupe events to deliver. For example, when
  a publish event comes in, that publish request should be de-duped immediately.
  Later, when the feed puller comes through to grab feed diffs, it should insert
  a single event to deliver, collapsing any overlapping publish events during
  the delay from publish time to feed pulling time.
  """

  topic = db.TextProperty(required=True)
  topic_hash = db.StringProperty(required=True)
  payload = db.TextProperty(required=True)
  last_callback = db.TextProperty(default="")  # For paging Subscriptions
  failed_callbacks = db.ListProperty(db.Key)  # Refs to Subscription entities
  last_modified = db.DateTimeProperty(auto_now=True)

  @classmethod
  def create_event_for_topic(cls, topic, header_footer, entry_list):
    """Creates an event to deliver for a topic and set of published entries.
    
    Args:
      topic: The topic that had the event.
      header_footer: The header and footer of the published feed into which
        the entry list will be spliced.
      entry_list: List of FeedEntryRecord entities to deliver in this event,
        in order of newest to oldest.
    
    Returns:
      A new EventToDeliver instance that has not been stored.
    """
    # TODO: Make this work for both RSS and Atom
    close_index = header_footer.rfind('</feed>')
    assert close_index != -1, 'Could not find </feed> in header/footer data'
    payload_list = ['<?xml version="1.0" encoding="utf-8"?>',
                    header_footer[:close_index]]
    for entry in entry_list:
      payload_list.append(entry.entry_payload)
    payload_list.append('</feed>')
    payload = '\n'.join(payload_list)

    return cls(topic=topic,
               topic_hash=sha1_hash(topic),
               payload=payload)

  def update(self, more_callbacks, last_callback, more_failed_callbacks):
    """Updates an event with work progress or deletes it if it's done.
    
    Also deletes the ownership memcache entry for this work item so it will
    be picked up again the next time get_work() is called.
    
    Args:
      more_callbacks: True if there are more callbacks to deliver, False if
        there are no more subscribers to deliver for this feed.
      last_callback: The last callback to be contacted while going through the
        work during the most recent iteration.
      more_failed_callbacks: List of Subscription entities for this event that
        failed to deliver.
    """
    if not more_callbacks:
      # TODO: Correctly handle when there are no more callbacks, but
      # 'self.failed_callbacks' has stuff in it.
      self.delete()
    else:
      self.last_callback = last_callback
      self.failed_callbacks.extend(e.key() for e in more_failed_callbacks)
      self.put()
      memcache.delete(str(self.key()))

  @classmethod
  def get_work(cls):
    """Retrieves a pending event to deliver.
    
    Returns:
      An EventToDeliver instance, or None if no work is available.
    """
    return query_and_own(cls, 'ORDER BY last_modified ASC',
                         LEASE_PERIOD_SECONDS)

################################################################################
# Subscription handlers and workers

class SubscribeHandler(webapp.RequestHandler):
  def get(self):
    self.response.out.write(template.render('subscribe_debug.html', {}))

  def post(self):
    # TODO: Update this to match the design doc

    callback = self.request.get('callback', '').lower()
    topic = self.request.get('topic', '').lower()
    async = self.request.get('async', '').lower()
    mode = self.request.get('mode', '').lower()
    # TODO: Error handling, input validation
    if not (callback and topic and async and mode):
      return self.error(500)

    # TODO: Verify the callback

    # TODO: exception handling
    if mode == 'subscribe':
      if async.startswith('s'):
        if Subscription.insert(callback, topic):
          self.response.out.write('Sync: Created subscription.')
        else:
          self.response.out.write('Sync: Subscription exists.')
      else:
        if Subscription.request_insert(callback, topic):
          self.response.out.write('Async: Created subscribe request.')
        else:
          self.response.out.write('Async: Subscribe request unnecessary.')
    else:
      if async.startswith('s'):
        if Subscription.remove(callback, topic):
          self.response.out.write('Sync: Removed subscription.')
        else:
          self.response.out.write('Sync: No subscription to remove.')
      else:
        if Subscription.request_remove(callback, topic):
          self.response.out.write('Async: Created unsubscribe request.')
        else:
          self.response.out.write('Async: Unsubscribe request unnecessary.')


################################################################################
# Publishing handlers and workers

class PublishHandler(webapp.RequestHandler):
  """End-user accessible handler for the Publish event."""

  def get(self):
    self.response.out.write(template.render('publish_debug.html', {}))

  def post(self):
    self.response.headers['Content-Type'] = 'text/plain'

    mode = self.request.get('hub.mode')
    if mode.lower() != 'publish':
      self.response.set_status(400)
      self.response.out.write('hub.mode MUST be "publish"')
      return

    urls = self.request.get_all('hub.url')
    if not urls:
      self.response.set_status(400)
      self.response.out.write('MUST supply at least one hub.url parameter')
      return
    
    # TODO: Possibly limit the number of URLs to publish at a time?
    logging.info('Publish event for URLs: %s', urls)

    # Record all FeedToFetch requests here. The background Pull worker will
    # verify if there are any subscribers that need event delivery and will
    # skip any unused feeds.
    try:
      FeedToFetch.insert(urls)
    except (apiproxy_errors.Error, db.Error):
      logging.exception('Failed to insert FeedToFetch records')
      self.response.headers['Retry-After'] = '120'
      self.response.set_status(503)
      self.response.out.write('Transient error; please try again later')
    else:
      self.response.set_status(204)


class PullFeedHandler(webapp.RequestHandler):
  """Background worker for pulling feeds."""
  
  def __init__(self, filter_feed=feed_diff.filter):
    """Initializer."""
    webapp.RequestHandler.__init__(self)
    self.filter_feed = filter_feed
  
  def get(self):
    work = FeedToFetch.get_work()
    if not work:
      logging.info('No feeds to fetch.')
      return

    # TODO: correctly handle when feed fetching fails. have a maximum number
    # of retries before we give up and mark the feed as bad (and put it on
    # probation for some amount of time). Maybe have exponential back-off on
    # the number of retries that we'll do after subsequent failures?
    logging.info('Fetching topic %s', work.topic)
    try:
      response = urlfetch.fetch(work.topic)
    except (apiproxy_errors.Error, urlfetch.Error):
      logging.exception('Failed to fetch feed')
      return

    if response.status_code != 200:
      logging.error('Received bad status_code=%s', response.status_code)
      return

    # Parse the feed.
    header_footer, entries_map = self.filter_feed('', response.content)

    # Find the new entries we've never seen before, and any entries that we
    # knew about that have been updated.
    existing_entries = FeedEntryRecord.get_entries_for_topic(
        work.topic, entries_map.keys())
    existing_dict = dict((e.entry_id, (e.entry_updated, e.entry_payload))
                         for e in existing_entries if e)

    logging.info('Retrieved %d feed entries, %d of which have been seen before',
                 len(entries_map), len(existing_dict))

    entities_to_save = []
    for entry_id, (new_updated, new_content) in entries_map.iteritems():
      try:
        old_updated, old_content = existing_dict[entry_id]
        # Only mark the entry as new if the update time is newer.
        # TODO: Maybe we want to mark all updated time changes as interesting?
        # TODO: Maybe mark it as updated even if the only change is the content?
        # TODO: Maybe keep track of a digest of the entry_payload so we can
        # figure out when the content changes easier?
        if old_updated >= new_updated:
          continue
      except KeyError:
        pass

      entities_to_save.append(FeedEntryRecord.create_entry_for_topic(
          work.topic, entry_id, new_updated, new_content))

    # If there are no new entries, then we're done. Otherwise, we need to
    # mark the whole feed as updated.
    if not entities_to_save:
      logging.info('No new entries found')
    else:
      logging.info('Saving %d new/updated entries', len(entities_to_save))
      # Ensure the entries are in descending date order, newest first.
      entities_to_save.sort(key=lambda x: getattr(x, 'entry_updated'),
                            reverse=True)

      # Batch put all of this data and complete the work.
      # TODO: Better error handling. Maybe have EventToDeliver have a unique
      # key name so any failure at this point is idempotent. Only the
      # FeedToFetch instances will need an update time to help set the key
      # of the EventToDeliver?
      entities_to_save.append(EventToDeliver.create_event_for_topic(
          work.topic, header_footer, entities_to_save))
      entities_to_save.append(FeedRecord.create_record(
          work.topic, header_footer))
      db.put(entities_to_save)

    work.delete()

################################################################################

class PushEventHandler(webapp.RequestHandler):
  def get(self):
    work = EventToDeliver.get_work()
    if not work:
      logging.debug('No events to deliver.')
      return

    # Retrieve the first N + 1 subscribers; note if we have more to contact.
    subscriber_list = Subscription.get_subscribers(
        work.topic, EVENT_SUBSCRIBER_CHUNK_SIZE + 1,
        starting_at_callback=work.last_callback)
    more_subscribers = len(subscriber_list) > EVENT_SUBSCRIBER_CHUNK_SIZE
    last_callback = ''
    if subscriber_list:
      last_callback = subscriber_list[-1].callback
    subscriber_list = subscriber_list[:EVENT_SUBSCRIBER_CHUNK_SIZE]
    logging.info('%d more subscribers to contact for topic %s',
                 len(subscriber_list), work.topic)

    # Keep track of broken callbacks for try later.
    broken_callbacks = []
    def callback(url, result, exception):
      if exception or result.status_code not in (200, 204):
        logging.warning('Could not deliver to target url %s: '
                        'Exception = %r, status_code = %s',
                        url, exception, result.status_code)
        broken_callbacks.append(url)
    def create_callback(url):
      return lambda *args: callback(url, *args)

    for subscriber in subscriber_list:
      urlfetch_async.fetch(subscriber.callback,
                           method='POST',
                           headers={'content-type': 'application/atom+xml'},
                           payload=work.payload.encode('utf-8'),
                           async_proxy=async_proxy,
                           callback=create_callback(subscriber.callback))
    async_proxy.wait()
    work.update(more_subscribers, last_callback, broken_callbacks)

################################################################################

class HomepageHandler(webapp.RequestHandler):
  def get(self):
    self.response.out.write(template.render('welcome.html', {}))

################################################################################

def main():
  application = webapp.WSGIApplication([
    (r'/', HomepageHandler),
    (r'/subscribe', SubscribeHandler),
    (r'/publish', PublishHandler),
    (r'/work/pull_feeds', PullFeedHandler),
    (r'/work/push_events', PushEventHandler),
  ],debug=True)
  wsgiref.handlers.CGIHandler().run(application)


if __name__ == '__main__':
  main()
