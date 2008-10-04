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
import random
import urllib
import wsgiref.handlers

from google.appengine.api import datastore_types
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template

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

# How many entities to retrieve when doing QueryAndOwn() on pending queues.
QUERY_AND_OWN_SIZE = 50

# How many entities to try to lock when doing QueryAndOwn().
QUERY_AND_OWN_TRY_LOCK_SIZE = 5

# How long to hold a lock after QueryAndOwn(), in seconds.
LEASE_PERIOD_SECONDS = 15

# How many subscribers to contact at a time when delivering events.
EVENT_SUBSCRIBER_CHUNK_SIZE = 50

################################################################################
# Helper functions

def Sha1Hash(value):
  """Returns the sha1 hash of the supplied value."""
  return hashlib.sha1(value).hexdigest()


def GetHashKeyName(value):
  """Returns a valid entity key_name that's a hash of the supplied value."""
  return 'hash_' + Sha1Hash(value)


def QueryAndOwn(model_class, gql_query):
  """Query for work to do and temporarily own it.
  
  Args:
    model_class: The db.Model sub-class that contains the work to do.
    gql_query: String containing the GQL query that will retrieve the work
      to do in order of priority for this model_class.
  
  Returns:
    A model_class instance, if work could be retrieved, or None if there is
    no work to do or work could not be retrieved (due to collisions, etc).
  """
  work_to_do = model_class.gql(gql_query).fetch(QUERY_AND_OWN_SIZE)
  if not work_to_do:
    return None

  possible_work = random.sample(work_to_do,
      min(len(work_to_do), QUERY_AND_OWN_TRY_LOCK_SIZE))
  for work in possible_work:
    if memcache.add(str(work.key()), 'owned', time=LEASE_PERIOD_SECONDS):
      return work
  
  return None

################################################################################
# Models

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
    return GetHashKeyName('%s\n%s' % (callback, topic))

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
                  callback_hash=Sha1Hash(callback),
                  topic=topic,
                  topic_hash=Sha1Hash(topic),
                  expiration_time=datetime.datetime.now() + EXPIRATION_DELTA)
      sub.subscription_state = cls.STATE_VERIFIED
      sub.put()
      return sub_is_new
    return db.run_in_transaction(txn)

  @classmethod
  def request_insert(cls, callback, topic, **kwargs):
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
                  callback_hash=Sha1Hash(callback),
                  topic=topic,
                  topic_hash=Sha1Hash(topic),
                  expiration_time=datetime.datetime.now() + EXPIRATION_DELTA,
                  **kwargs)
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
    """Check if a topic URL has subscribers.
    
    Args:
      topic: The topic URL to check for subscribers.
    
    Returns:
      True if it has subscribers, False otherwise.
    """
    if cls.all().filter('topic_hash =', Sha1Hash(topic)).get() is not None:
      return True
    else:
      return False

  @classmethod
  def get_subscribers(cls, topic, count, starting_at_callback_hash=None):
    """Gets the list of subscribers starting at an offset.
    
    Args:
      topic: The topic URL to retrieve subscribers for.
      count: How many subscribers to retrieve.
      starting_at_callback: A string containing the callback hash to offset
        to when retrieving more subscribers. If None, then subscribers will
        be retrieved from the beginning
    """
    query = cls.all()
    query.filter('topic_hash =', Sha1Hash(topic))
    if starting_at_callback:
      query.filter('callback_hash =', Sha1Hash(starting_at_callback))
    query.order('callback_hash')
    
    return query.fetch(count)


class FeedToFetch(db.Model):
  """A feed that has new data that needs to be pulled.
  
  The key name of this entity is a GetHashKeyName() hash of the topic URL.
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
    feed_list = [cls(key_name=GetHashKeyName(topic), topic=topic)
                 for topic in topic_list]
    db.put(feed_list)

  @classmethod
  def get_work(cls):
    """Retrieves a feed to fetch and owns it by acquiring a temporary lock.
    
    Returns:
      A FeedToFetch entity that has been owned, or None if there is currently
      no work to do.
    """
    return QueryAndOwn(cls, 'ORDER BY update_time ASC')


class FeedRecord(db.Model):
  """Represents the content of a feed without any entries.
  
  This is everything in a feed except for the entry data. That means any
  footers, top-level XML elements, namespace declarations, etc, will be
  captured in this entity.

  The key name of this entity is a GetHashKeyName() of the topic URL.
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
    return cls.get_by_key_name(GetHashKeyName(topic))
  
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
    return cls(key_name=GetHashKeyName(topic),
               topic=topic,
               topic_hash=Sha1Hash(topic),
               header_footer=header_footer)


class FeedEntryRecord(db.Model):
  """Represents a feed entry that has been seen.
  
  The key name of this entity is a GetHashKeyName() hash of the combination
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
    return GetHashKeyName('%s\n%s' % (topic, entry_id))
  
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
      xml_data: String containing the entry XML data.

    Returns:
      A new FeedEntryRecord that should be inserted into the Datastore.
    """
    return cls(key_name=cls.create_key_name(topic, entry_id),
               topic=topic,
               topic_hash=Sha1Hash(topic),
               entry_id=entry_id,
               entry_id_hash=Sha1Hash(entry_id),
               entry_updated=updated,
               entry_payload=xml_data)


class EventToDeliver(db.Model):
  """TODO
  """

  topic = db.TextProperty(required=True)
  topic_hash = db.StringProperty(required=True)
  payload = db.TextProperty(required=True)
  last_callback_hash = db.StringProperty(default="")  # For paging
  failed_callbacks = db.ListProperty(db.Key)  # Refs to Subscription entities
  last_modified = db.DateTimeProperty(auto_now=True)
  
  @classmethod
  def create_event_for_topic(cls, topic, header_footer, entry_list):
    """TODO
    """
    # TODO: Make this work for both RSS and Atom
    close_index = header_footer.find('</feed>')
    payload_list = ['<?xml version="1.0" encoding="utf-8"?>',
                    header_footer[:close_index]]
    for entry in entry_list:
      payload_list.append(entry.entry_payload)
    payload_list.append('</feed>')
    payload = '\n'.join(payload_list)

    return cls(key_name=GetHashKeyName(topic),
               topic=topic,
               topic_hash=Sha1Hash(topic),
               payload=payload,
               last_callback_hash="")
  
  def update(self, more_callbacks, last_callback_hash, more_failed_callbacks):
    """TODO
    """
    if not more_callbacks:
      # TODO: Correctly handle when there are no more callbacks, but
      # 'more_failed_callbacks' has stuff in it.
      self.delete()
    else:
      self.last_callback_hash = last_callback_hash
      self.failed_callbacks.append(failed_callbacks)
      self.put()

  @classmethod
  def get_work(cls):
    """TODO
    """
    return QueryAndOwn(cls, 'ORDER BY last_modified ASC')

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
  def get(self):
    self.response.out.write(template.render('publish_debug.html', {}))
  
  def post(self):
    mode = self.request.get('hub.mode')
    assert mode.lower() == 'publish'
    urls = self.request.get_all('hub.url')
    
    logging.info('Got publish urls for %s', urls)

    # TODO: validate urls? probably not needed, since we can just validate
    # when the original subscription is made.

    new_topics = []
    for topic_url in urls:
      if not Subscription.has_subscribers(topic_url):
        logging.info('topic_url="%s" has no subscribers', topic_url)
      else:
        new_topics.append(topic_url)
    
    FeedToFetch.insert(new_topics)
    self.response.set_status(204)


class PullFeedHandler(webapp.RequestHandler):
  def get(self):
    work = FeedToFetch.get_work()
    if not work:
      logging.info('No feeds to fetch.')
      return

    # TODO: correctly handle when feed fetching fails. have a maximum number
    # of retries before we give up and mark the feed as bad (and put it on
    # probation for some amount of time).
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
    header_footer, entries_map = feed_diff.filter('', response.content)

    # Find the new entries we've never seen before, and any entries that we
    # knew about that have been updated.
    existing_entries = FeedEntryRecord.get_entries_for_topic(
        work.topic, entries_map.keys())
    existing_dict = dict((e.entry_id, (e.entry_updated, e.entry_payload))
                         for e in existing_entries if e)

    logging.info('Retrieved %d entries, %d of which have been seen before',
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
      # Batch put all of this data and complete the work.
      # TODO: Also put the notification event entities.
      # TODO: Error handling
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
      logging.info('No events to deliver.')
      return
    
    # Retrieve the first N + 1 subscribers; note if we have more to contact.
    subscriber_list = Subscription.gql(
        'WHERE callback_hash > :1 ORDER BY callback_hash ASC',
        work.last_callback_hash).fetch(EVENT_SUBSCRIBER_CHUNK_SIZE + 1)
    if not subscriber_list:
      logging.info('No subscribers for topic %s', work.topic)
      return

    more_subscribers = len(subscriber_list) > EVENT_SUBSCRIBER_CHUNK_SIZE
    more_callback_hash = subscriber_list[-1].callback_hash
    subscriber_list[:EVENT_SUBSCRIBER_CHUNK_SIZE]
    logging.info('%d subscribers to contact for topic %s',
                 len(subscriber_list), work.topic)

    # Keep track of broken callbacks for try later.
    broken_callbacks = []
    def callback(url, result, exception):
      if exception or result.status_code != 200:
        broken_callbacks.append(url)

    def create_callback(url):
      return lambda *args: callback(url, *args)

    headers = {'content-type': 'application/x-www-form-urlencoded'}
    post_params = {'content': work.payload.encode('utf-8')}
    payload = urllib.urlencode(post_params)

    for subscriber in subscriber_list:
      urlfetch_async.fetch(subscriber.callback,
                           method='POST',
                           payload=payload,
                           async_proxy=async_proxy,
                           callback=create_callback(subscriber.callback))
    async_proxy.wait()
    work.update(more_subscribers, more_callback_hash, broken_callbacks)

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
