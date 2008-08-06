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

import datetime
import hashlib
import logging
logging.getLogger().setLevel(logging.DEBUG)
import wsgiref.handlers

from google.appengine.api import urlfetch
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import urlfetch_service_pb
from google.appengine.api.urlfetch_errors import *
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext import db
from google.appengine.runtime import apiproxy_errors

import urlfetch_async
import async_apiproxy

async_proxy = async_apiproxy.AsyncAPIProxy()


EXPIRATION_DELTA = datetime.timedelta(days=90)


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

  callback_url = db.TextProperty(required=True)
  topic = db.StringProperty(required=True)
  created_time = db.DateTimeProperty(auto_now_add=True)
  expiration_time = db.DateTimeProperty(required=True)
  subscription_state = db.StringProperty(default=STATE_NOT_VERIFIED)

  @staticmethod
  def create_key_name(callback_url, topic):
    combined_id = '%s\n%s' % (callback_url, topic)
    return 'hash_' + hashlib.sha1(combined_id).hexdigest()

  @classmethod
  def insert(cls, callback_url, topic):
    """Marks a callback URL as being subscribed to a topic.
    
    Creates a new subscription if None already exists. Forces any existing,
    pending request (i.e., async) to immediately enter the verified state.
    
    Args:
      callback_url: URL that will receive callbacks.
      topic: The topic to subscribe to.
    
    Returns:
      True if the subscription was newly created, False otherwise.
    """
    key_name = cls.create_key_name(callback_url, topic)
    def txn():
      sub_is_new = False
      sub = cls.get_by_key_name(key_name)
      if sub is None:
        sub_is_new = True
        sub = cls(key_name=key_name,
                  callback_url=callback_url,
                  topic=topic,
                  expiration_time=datetime.datetime.now() + EXPIRATION_DELTA)
      sub.subscription_state = cls.STATE_VERIFIED
      sub.put()
      return sub_is_new
    return db.run_in_transaction(txn)

  @classmethod
  def request_insert(cls, callback_url, topic, **kwargs):
    """Records that a callback URL needs verification before being subscribed.
    
    Creates a new subscription request (for asynchronous verification) if None
    already exists. Any existing subscription request will not be modified;
    for instance, if a subscription has already been verified, this method
    will do nothing.
    
    Args:
      callback_url: URL that will receive callbacks.
      topic: The topic to subscribe to.
    
    Returns:
      True if the subscription request was newly created, False otherwise.
    """
    key_name = cls.create_key_name(callback_url, topic)
    def txn():
      sub_is_new = False
      sub = cls.get_by_key_name(key_name)
      if sub is None:
        sub_is_new = True
        sub = cls(key_name=key_name,
                  callback_url=callback_url,
                  topic=topic,
                  expiration_time=datetime.datetime.now() + EXPIRATION_DELTA,
                  **kwargs)
        sub.put()
      return sub_is_new
    return db.run_in_transaction(txn)

  @classmethod
  def remove(cls, callback_url, topic):
    """Causes a callback URL to no longer be subscribed to a topic.
    
    If the callback_url was not already subscribed to the topic, this method
    will do nothing. Otherwise, the subscription will immediately be removed.
    
    Args:
      callback_url: URL that will receive callbacks.
      topic: The topic to subscribe to.
    
    Returns:
      True if the subscription had previously existed, False otherwise.
    """
    key_name = cls.create_key_name(callback_url, topic)
    def txn():
      sub = cls.get_by_key_name(key_name)
      if sub is not None:
        sub.delete()
        return True
      return False
    return db.run_in_transaction(txn)

  @classmethod
  def request_remove(cls, callback_url, topic):
    """Records that a callback URL needs to be unsubscribed.
    
    Creates a new request to unsubscribe a callback URL from a topic (where
    verification should happen asynchronously). If an unsubscribe request
    has already been made, this method will do nothing.
    
    Args:
      callback_url: URL that will receive callbacks.
      topic: The topic to subscribe to.
    
    Returns:
      True if the unsubscribe request is new, False otherwise (i.e., a request
      for asynchronous unsubscribe was already made).
    """
    key_name = cls.create_key_name(callback_url, topic)
    def txn():
      sub = cls.get_by_key_name(key_name)
      if sub is not None and sub.subscription_state != cls.STATE_TO_DELETE:
        sub.subscription_state = cls.STATE_TO_DELETE
        sub.put()
        return True
      return False
    return db.run_in_transaction(txn)


class SubscribeHandler(webapp.RequestHandler):
  def get(self):
    self.response.out.write(template.render('subscribe_debug.html', {}))
  
  def post(self):
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


class UrlFetchTestHandler(webapp.RequestHandler):
  def get(self):
    self.response.out.write('Hello world!')
    # start async fetch:
    self.start_async_fetch("http://bradfitz.com/test/1.txt")
    self.start_async_fetch("http://bradfitz.com/test/2.txt")
    async_proxy.wait()

  def start_async_fetch(self, url):
    def callback(result, exception):
      self.on_url(url, result, exception)
    urlfetch_async.fetch(url, async_proxy=async_proxy, callback=callback)

  def on_url(self, url, result, exception):
    logging.info('Callback received for "%s": %s', url, result.status_code)
    if result:
      self.response.out.write("<p>Got content: " + result.content + "</p>\n")
    else:
      self.response.out.write("Got exception!")


def main():
  application = webapp.WSGIApplication([
    (r'/subscribe', SubscribeHandler),
    (r'/async_fetch', UrlFetchTestHandler),
  ],debug=True)
  wsgiref.handlers.CGIHandler().run(application)



if __name__ == '__main__':
  main()
