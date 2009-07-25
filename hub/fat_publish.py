#!/usr/bin/env python
#
# Copyright 2009 Google Inc.
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

"""Fat publish receiver for working around caching issues with the demo hub.

This isn't part of the PubSubHubbub spec. This extension hook is useful for
publishers who want to try out the PubSubHubbub protocol without building or
running their own hub at first. Receiving Fat publish events directly from
publishers allows the Hub to work around any replication/caching delays that
are described in this wiki entry (see the section on multiple datacenters):

  http://code.google.com/p/pubsubhubbub/wiki/PublisherEfficiency

To enable this module, set the SHARED_SECRET below to something safe and then
symlink the fat_publish.py file into the 'hooks' directory before deploying.

To use this module as a client, send POST requests with the parameters
'topic', 'content', and 'signature'. They should look like this:

  POST /fatping HTTP/1.1
  Content-Type: application/x-www-form-urlencoded
  Content-Length: ...

  topic=http%3A%2F%2Fexample.com%2Fmytopic&\
  content=<url escaped feed contents>&\
  signature=<hmac signature of content and topic concatenated">
"""

import logging

from google.appengine.ext import webapp


# The default shared secret used by the Hub and the publisher.
SHARED_SECRET = 'replaceme'


# Define the Hook class for testing.
if 'register' not in globals():
  class Hook(object):
    pass


class FatPublishHandler(webapp.RequestHandler):
  """Request handler for receiving fat publishes.
  
  Returns 204 on success, 403 on auth errors, 400 on bad feeds, 500 on
  any other type of error.
  """

  secret = None

  def post(self):
    topic = self.request.get('topic')
    content = self.request.get('content')
    signature = self.request.get('signature')

    if not (topic and content and signature):
      error_message = (
          'Fat publish must have required parameters in urlencoded format: '
          '"topic", "content", "signature"')
      logging.error(error_message)
      self.response.set_status(400)
      self.response.out.write(error_message)
      return

    logging.debug('Fat publish for topic=%s, signature=%s, size=%s',
                  topic, signature, len(content))

    if not Subscription.has_subscribers(topic):
      logging.debug('Ignoring fat publish because there are no subscribers.')
      self.response.set_status(204)
      return 

    logging.info('Subscribers found. Accepting fat publish event.')
    expected_signature = sha1_hmac(self.secret, content + topic)
    if expected_signature != signature:
      error_message = (
          'Received fat publish with invalid signature. '
          'expected=%s, found=%s' % (expected_signature, signature))
      logging.error(error_message)
      self.response.set_status(403)
      self.response.out.write(error_message)
      return

    feed_record = FeedRecord.get_or_create(topic)
    if parse_feed(feed_record, self.request.headers, content):
      self.response.set_status(204)
    else:
      self.response.out.write('Could not parse or save feed updates.')
      self.response.set_status(400)


def create_handler(shared_secret):
  """Creates a FatPublishHandler sub-class with a particular shared secret.

  Args:
    shared_secret: Used to verify the authenticity of fat publishes.
  """
  class SpecificFatPublishHandler(FatPublishHandler):
    secret = shared_secret
  return SpecificFatPublishHandler


class FatPublishHook(Hook):
  """Hook for accepting fat publishes from publishers."""

  def __init__(self, handler):
    """Initializer.

    Args:
      handler: FatPublishHandler class to add for fatpinging.
    """
    self.handler = handler

  def inspect(self, args, kwargs):
    """Adds the FatPublishHandler to the list of request handlers."""
    args[0].append((r'/fatping', self.handler))
    return False


if 'register' in globals():
  # You can re-register this same hook here with different shared secrets if
  # you would like to allow other publishing endpoints to do the same thing
  # with separate access controls.
  register(modify_handlers, FatPublishHook(create_handler(SHARED_SECRET)))
