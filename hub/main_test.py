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

"""Tests for the main module."""

import datetime
import logging
logging.basicConfig(format='%(levelname)-8s %(filename)s] %(message)s')
import os
import sys
import unittest

import testutil
testutil.fix_path()


from google.appengine import runtime
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.runtime import apiproxy_errors

import async_apiproxy
import main
import urlfetch_test_stub

################################################################################

class UtilityFunctionTest(unittest.TestCase):
  """Tests for utility functions."""
  
  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()

  def testSha1Hash(self):
    self.assertEquals('09f2c66851e75a7800748808ae7d855869b0c9d7',
                      main.sha1_hash('this is my test data'))

  def testGetHashKeyName(self):
    self.assertEquals('hash_54f6638eb67ad389b66bbc3fa65f7392b0c2d270',
                      main.get_hash_key_name('and now testing a key'))

  def testIsValidUrl(self):
    self.assertTrue(main.is_valid_url(
        'https://example.com:443/path/to?handler=1&b=2'))
    self.assertFalse(main.is_valid_url('httpm://example.com'))
    self.assertFalse(main.is_valid_url('http://example.com:8080'))
    self.assertFalse(main.is_valid_url('http://example.com/blah#bad'))

################################################################################

class TestWorkQueueHandler(webapp.RequestHandler):
  @main.work_queue_only
  def get(self):
    self.response.out.write('Pass')


class WorkQueueOnlyTest(testutil.HandlerTestBase):
  """Tests the @work_queue_only decorator."""

  handler_class = TestWorkQueueHandler

  def testNotLoggedIn(self):
    os.environ['SERVER_SOFTWARE'] = 'Production'
    self.handle('get')
    self.assertEquals(302, self.response_code())

  def testCronHeader(self):
    os.environ['SERVER_SOFTWARE'] = 'Production'
    os.environ['HTTP_X_APPENGINE_CRON'] = 'True'
    try:
      self.handle('get')
      self.assertEquals('Pass', self.response_body())
    finally:
      del os.environ['HTTP_X_APPENGINE_CRON']

  def testDevelopmentEnvironment(self):
    os.environ['SERVER_SOFTWARE'] = 'Development/1.0'
    self.handle('get')
    self.assertEquals('Pass', self.response_body())

  def testAdminUser(self):
    os.environ['SERVER_SOFTWARE'] = 'Production'
    os.environ['USER_EMAIL'] = 'foo@example.com'
    os.environ['USER_IS_ADMIN'] = '1'
    try:
      self.handle('get')
      self.assertEquals('Pass', self.response_body())
    finally:
      del os.environ['USER_IS_ADMIN']

  def testNonAdminUser(self):
    os.environ['SERVER_SOFTWARE'] = 'Production'
    os.environ['USER_EMAIL'] = 'foo@example.com'
    os.environ['USER_IS_ADMIN'] = '0'
    try:
      self.handle('get')
      self.assertEquals(401, self.response_code())
    finally:
      del os.environ['USER_IS_ADMIN']

################################################################################

class TestWork(db.Model):
  index = db.IntegerProperty(required=True)


class QueryAndOwnTest(unittest.TestCase):
  
  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.test_work = [TestWork(index=0), TestWork(index=1), TestWork(index=2)]
    self.query = 'ORDER BY index DESC'
  
  def put_test_work(self):
    for work in self.test_work:
      work.put()

  def testQueryAndOwn_noWork(self):
    work = main.query_and_own(TestWork, self.query, 60, work_count=1)
    self.assertTrue(work is None)
    work = main.query_and_own(TestWork, self.query, 60, work_count=5)
    self.assertEquals([], work)

  def testQueryAndOwn_singleItem(self):
    self.put_test_work()
    # Retrieve work successfully.
    work = main.query_and_own(TestWork, self.query, 60)
    work_key = str(work.key())
    self.assertEquals('owned', memcache.get(work_key))

    # Verify that the work cannot be owned again without memcache expiry.
    other_query = 'WHERE index = %d' % work.index
    other_work = main.query_and_own(TestWork, other_query, 60)
    self.assertTrue(other_work is None)
    self.assertTrue(memcache.delete(work_key))
    other_work = main.query_and_own(TestWork, other_query, 60)
    self.assertEquals(other_work.key(), work.key())
    self.assertEquals('owned', memcache.get(work_key))

    # Verify that other work that's not already owned can be owned. We know
    # this test is valid because the lock_ratio means that we will try to lock
    # them all.
    more_work = main.query_and_own(TestWork, self.query, 60,
                                  lock_ratio=len(self.test_work))
    self.assertNotEquals(more_work.key(), work.key())
    self.assertEquals('owned', memcache.get(str(more_work.key())))
    for w in self.test_work:
      if w.key() != more_work.key() and w.key() != work.key():
        self.assertTrue(memcache.get(str(w.key())) is None)

  def testQueryAndOwn_multipleItem(self):
    self.put_test_work()
    # Retrieving multiple items works just fine.
    work = main.query_and_own(TestWork, self.query, 60, work_count=3)
    self.assertEquals(set(w.key() for w in work),
                      set(w.key() for w in self.test_work))
    for w in self.test_work:
      self.assertEquals('owned', memcache.get(str(w.key())))
    
    # Re-acquiring a sub-set of the work_count.
    redo_work = work[1]
    redo_work_key = str(redo_work.key())
    self.assertTrue(memcache.delete(redo_work_key))
    more_work = main.query_and_own(TestWork, self.query, 60, work_count=3)
    self.assertEquals(1, len(more_work))
    self.assertEquals(redo_work.key(), more_work[0].key())
    self.assertEquals('owned', memcache.get(redo_work_key))

################################################################################

KnownFeed = main.KnownFeed

class KnownFeedTest(unittest.TestCase):
  """Tests for the KnownFeed model class."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()

  def testCreateAndDelete(self):
    topic = 'http://example.com/my-topic'
    known_feed = KnownFeed.create(topic)
    self.assertEquals(topic, known_feed.topic)
    db.put(known_feed)

    found_feed = db.get(KnownFeed.create_key(topic))
    self.assertEquals(found_feed.key(), known_feed.key())
    self.assertEquals(found_feed.topic, known_feed.topic)

    db.delete(KnownFeed.create_key(topic))
    self.assertTrue(db.get(KnownFeed.create_key(topic)) is None)


################################################################################

Subscription = main.Subscription


class SubscriptionTest(unittest.TestCase):
  """Tests for the Subscription model class."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.callback = 'http://example.com/my-callback-url'
    self.callback2 = 'http://example.com/second-callback-url'
    self.callback3 = 'http://example.com/third-callback-url'
    self.topic = 'http://example.com/my-topic-url'
    self.topic2 = 'http://example.com/second-topic-url'
    self.callback_key_map = dict(
        (Subscription.create_key_name(cb, self.topic), cb)
        for cb in (self.callback, self.callback2, self.callback3))

  def get_subscription(self):
    """Returns the subscription for the test callback and topic."""
    return Subscription.get_by_key_name(
        Subscription.create_key_name(self.callback, self.topic))

  def testRequestInsert_defaults(self):
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, 'token'))
    self.assertFalse(Subscription.request_insert(
        self.callback, self.topic, 'token'))
    self.assertEquals(Subscription.STATE_NOT_VERIFIED,
                      self.get_subscription().subscription_state)
  
  def testInsert_defaults(self):
    self.assertTrue(Subscription.insert(self.callback, self.topic))
    self.assertFalse(Subscription.insert(self.callback, self.topic))
    self.assertEquals(Subscription.STATE_VERIFIED,
                      self.get_subscription().subscription_state)

  def testInsert_override(self):
    """Tests that insert will override the existing subscription state."""
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, 'token'))
    self.assertEquals(Subscription.STATE_NOT_VERIFIED,
                      self.get_subscription().subscription_state)
    self.assertFalse(Subscription.insert(self.callback, self.topic))
    self.assertEquals(Subscription.STATE_VERIFIED,
                      self.get_subscription().subscription_state)
  
  def testRemove(self):
    self.assertFalse(Subscription.remove(self.callback, self.topic))
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, 'token'))
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    self.assertFalse(Subscription.remove(self.callback, self.topic))
  
  def testRequestRemove(self):
    self.assertFalse(Subscription.request_remove(
        self.callback, self.topic, 'token'))
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, 'token'))
    self.assertTrue(Subscription.request_remove(
        self.callback, self.topic, 'token'))
    self.assertEquals(Subscription.STATE_TO_DELETE,
                      self.get_subscription().subscription_state)
    self.assertFalse(Subscription.request_remove(
        self.callback, self.topic, 'token'))

  def testHasSubscribers_unverified(self):
    """Tests that unverified subscribers do not make the subscription active."""
    self.assertFalse(Subscription.has_subscribers(self.topic))
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, 'token'))
    self.assertFalse(Subscription.has_subscribers(self.topic))

  def testHasSubscribers_verified(self):
    self.assertTrue(Subscription.insert(self.callback, self.topic))
    self.assertTrue(Subscription.has_subscribers(self.topic))
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    self.assertFalse(Subscription.has_subscribers(self.topic))
  
  def testGetSubscribers_unverified(self):
    """Tests that unverified subscribers will not be retrieved."""
    self.assertEquals([], Subscription.get_subscribers(self.topic, 10))
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, 'token'))
    self.assertTrue(Subscription.request_insert(
        self.callback2, self.topic, 'token'))
    self.assertTrue(Subscription.request_insert(
        self.callback3, self.topic, 'token'))
    self.assertEquals([], Subscription.get_subscribers(self.topic, 10))

  def testGetSubscribers_verified(self):
    self.assertEquals([], Subscription.get_subscribers(self.topic, 10))
    self.assertTrue(Subscription.insert(self.callback, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    sub_list = Subscription.get_subscribers(self.topic, 10)
    found_keys = set(s.key().name() for s in sub_list)
    self.assertEquals(set(self.callback_key_map.keys()), found_keys)

  def testGetSubscribers_count(self):
    self.assertTrue(Subscription.insert(self.callback, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    sub_list = Subscription.get_subscribers(self.topic, 1)
    self.assertEquals(1, len(sub_list))

  def testGetSubscribers_withOffset(self):
    """Tests the behavior of the starting_at_callback offset parameter."""
    # In the order the query will sort them.
    all_hashes = [
        u'87a74994e48399251782eb401e9a61bd1d55aeee',
        u'01518f29da9db10888a92e9f0211ac0c98ec7ecb',
        u'f745d00a9806a5cdd39f16cd9eff80e8f064cfee',
    ]
    all_keys = ['hash_' + h for h in all_hashes]
    all_callbacks = [self.callback_key_map[k] for k in all_keys]

    self.assertTrue(Subscription.insert(self.callback, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))

    def key_list(starting_at_callback):
      sub_list = Subscription.get_subscribers(
          self.topic, 10, starting_at_callback=starting_at_callback)
      return [s.key().name() for s in sub_list]

    self.assertEquals(all_keys, key_list(None))
    self.assertEquals(all_keys, key_list(all_callbacks[0]))
    self.assertEquals(all_keys[1:], key_list(all_callbacks[1]))
    self.assertEquals(all_keys[2:], key_list(all_callbacks[2]))

  def testGetSubscribers_multipleTopics(self):
    """Tests that separate topics do not overlap in subscriber queries."""
    self.assertEquals([], Subscription.get_subscribers(self.topic2, 10))
    self.assertTrue(Subscription.insert(self.callback, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    self.assertEquals([], Subscription.get_subscribers(self.topic2, 10))
    
    self.assertTrue(Subscription.insert(self.callback2, self.topic2))
    self.assertTrue(Subscription.insert(self.callback3, self.topic2))
    sub_list = Subscription.get_subscribers(self.topic2, 10)
    found_keys = set(s.key().name() for s in sub_list)
    self.assertEquals(
        set(Subscription.create_key_name(cb, self.topic2)
            for cb in (self.callback2, self.callback3)),
        found_keys)
    self.assertEquals(3, len(Subscription.get_subscribers(self.topic, 10)))

  def testGetConfirmWork(self):
    """Verifies that we can retrieve subscription confirmation work."""
    self.assertTrue(Subscription.request_insert(self.callback, self.topic,
                                                'token'))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    self.assertTrue(Subscription.request_remove(self.callback3, self.topic,
                                                'token'))
    work1 = Subscription.get_confirm_work()
    work2 = Subscription.get_confirm_work()
    self.assertNotEquals(work1.key(), work2.key())
    work3 = Subscription.get_confirm_work()
    self.assertTrue(work3 is None)

  def testConfirmFailed(self):
    """Tests retry delay periods when a subscription confirmation fails."""
    start = datetime.datetime.utcnow()
    def now():
      return start

    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, 'token'))
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    sub = Subscription.get_by_key_name(sub_key)
    self.assertEquals(0, sub.confirm_failures)

    self.assertTrue(Subscription.get_confirm_work() is not None)
    memcache.delete(str(sub.key()))

    for i, delay in enumerate((5, 10, 20, 40, 80)):
      sub.confirm_failed(max_failures=5, retry_period=5, now=now)
      self.assertEquals(sub.eta, start + datetime.timedelta(seconds=delay))
      self.assertEquals(i+1, sub.confirm_failures)
      # Getting work will yield no subscriptions here, since the ETA is
      # far into the future.
      self.assertTrue(Subscription.get_confirm_work(now=now) is None)

    # It will be deleted on the last try.
    sub.confirm_failed(max_failures=5, retry_period=5)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)

################################################################################

FeedToFetch = main.FeedToFetch

class FeedToFetchTest(unittest.TestCase):
  
  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.topic = 'http://example.com/topic-one'
    self.topic2 = 'http://example.com/topic-two'
    self.topic3 = 'http://example.com/topic-three'
  
  def testInsertAndGet(self):
    """Tests inserting and getting work."""
    all_topics = [self.topic, self.topic2, self.topic3]
    self.assertTrue(FeedToFetch.get_work() is None)
    FeedToFetch.insert(all_topics)
    found_topics = set()
    for i in xrange(len(all_topics)):
      feed = FeedToFetch.get_work()
      found_topics.add(feed.topic)
    self.assertEquals(set(all_topics), found_topics)
    self.assertTrue(FeedToFetch.get_work() is None)

  def testDuplicates(self):
    """Tests duplicate urls."""
    all_topics = [self.topic, self.topic, self.topic2, self.topic2]
    self.assertTrue(FeedToFetch.get_work() is None)
    FeedToFetch.insert(all_topics)
    found_topics = set()
    for i in xrange(len(set(all_topics))):
      feed = FeedToFetch.get_work()
      found_topics.add(feed.topic)
    print found_topics
    self.assertEquals(set(all_topics), found_topics)
    self.assertTrue(FeedToFetch.get_work() is None)

  def testFetchFailed(self):
    start = datetime.datetime.utcnow()
    def now():
      return start
    
    FeedToFetch.insert([self.topic])
    feed = FeedToFetch.get_work()
    for i, delay in enumerate((5, 10, 20, 40, 80)):
      feed.fetch_failed(max_failures=5, retry_period=5, now=now)
      self.assertEquals(feed.eta, start + datetime.timedelta(seconds=delay))
      self.assertEquals(i+1, feed.fetching_failures)
      self.assertEquals(False, feed.totally_failed)
      # Getting work will yield no feeds here, since the ETA is
      # far into the future.
      self.assertTrue(FeedToFetch.get_work(now=now) is None)

    feed.fetch_failed(max_failures=5, retry_period=5, now=now)
    self.assertEquals(True, feed.totally_failed)
    memcache.delete(str(feed.key()))
    self.assertTrue(FeedToFetch.get_work() is None)

################################################################################

FeedEntryRecord = main.FeedEntryRecord
EventToDeliver = main.EventToDeliver


class EventToDeliverTest(unittest.TestCase):
  
  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.topic = 'http://example.com/my-topic'
    # Order out of the datastore will be done by callback hash, not alphabetical
    self.callback = 'http://example.com/my-callback'
    self.callback2 = 'http://example.com/second-callback'
    self.callback3 = 'http://example.com/third-callback-123'
    self.callback4 = 'http://example.com/fourth-callback-1205'
    self.header_footer = '<feed>\n<stuff>blah</stuff>\n<xmldata/></feed>'
    self.test_entries = [
        FeedEntryRecord.create_entry_for_topic(self.topic, '1', 'time1',
                                               '<entry>article1</entry>'),
        FeedEntryRecord.create_entry_for_topic(self.topic, '2', 'time2',
                                               '<entry>article2</entry>'),
        FeedEntryRecord.create_entry_for_topic(self.topic, '3', 'time3',
                                               '<entry>article3</entry>'),
    ]

  def insert_subscriptions(self):
    """Inserts Subscription instances and an EventToDeliver for testing.
    
    Returns:
      Tuple (event, work_key, sub_list, sub_keys) where:
        event: The EventToDeliver that was inserted.
        work_key: Key for the 'event'
        sub_list: List of Subscription instances that were created in order
          of their callback hashes.
        sub_keys: Key instances corresponding to the entries in 'sub_list'.
    """
    event = EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries)
    event.put()
    work_key = str(event.key())

    Subscription.insert(self.callback, self.topic)
    Subscription.insert(self.callback2, self.topic)
    Subscription.insert(self.callback3, self.topic)
    Subscription.insert(self.callback4, self.topic)
    sub_list = Subscription.get_subscribers(self.topic, 10)
    sub_keys = [s.key() for s in sub_list]
    self.assertEquals(4, len(sub_list))

    return (event, work_key, sub_list, sub_keys)

  def testCreateEventForTopic(self):
    """Tests that the payload of an event is properly formed."""
    event = EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries)
    expected_data = \
u"""<?xml version="1.0" encoding="utf-8"?>
<feed>
<stuff>blah</stuff>
<xmldata/>
<entry>article1</entry>
<entry>article2</entry>
<entry>article3</entry>
</feed>"""
    self.assertEquals(expected_data, event.payload)

  def testCreateEvent_badHeaderFooter(self):
    """Tests when the header/footer data in an event is invalid."""
    self.assertRaises(AssertionError, EventToDeliver.create_event_for_topic,
        self.topic, '<feed>has no end tag', self.test_entries)

  def testNormal_noFailures(self):
    """Tests that event delivery with no failures will delete the event."""
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()
    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers()
    event.update(more, [])
    event = EventToDeliver.get(work_key)
    self.assertTrue(event is None)

  def testUpdate_failWithNoSubscribersLeft(self):
    """Tests that failures are written correctly by EventToDeliver.update.
    
    This tests the common case of completing the failed callbacks list extending
    when there are new Subscriptions that have been found in the latest work
    queue query.
    """
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    # Assert that the callback offset is updated, the work lock is cleared, and
    # any failed callbacks are recorded.
    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=1)
    event.update(more, [sub_list[0]])
    event = EventToDeliver.get(event.key())    
    self.assertEquals(EventToDeliver.NORMAL, event.delivery_mode)
    self.assertEquals([sub_list[0].key()], event.failed_callbacks)
    self.assertEquals(self.callback2, event.last_callback)
    self.assertTrue(memcache.get(work_key) is None)

    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=3)
    event.update(more, sub_list[1:])
    event = EventToDeliver.get(event.key())
    self.assertTrue(event is not None)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)
    self.assertEquals('', event.last_callback)
    
    self.assertEquals([s.key() for s in sub_list],
                      event.failed_callbacks)
    self.assertTrue(memcache.get(work_key) is None)
  
  def testUpdate_actuallyNoMoreCallbacks(self):
    """Tests when the normal update delivery has no Subscriptions left.
  
    This tests the case where update is called with no Subscribers in the
    list of Subscriptions. This can happen if a Subscription is deleted
    between when an update happens and when the work queue is invoked again.
    """
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=3)
    event.update(more, subs)
    event = EventToDeliver.get(event.key())
    self.assertEquals(self.callback4, event.last_callback)
    self.assertEquals(EventToDeliver.NORMAL, event.delivery_mode)

    # This final call to update will transition to retry properly.
    self.assertTrue(memcache.set(work_key, 'owned'))
    Subscription.remove(self.callback4, self.topic)
    more, subs = event.get_next_subscribers(chunk_size=1)
    event.update(more, [])
    event = EventToDeliver.get(event.key())
    self.assertEquals([], subs)
    self.assertTrue(event is not None)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)

  def testGetNextSubscribers_retriesFinallySuccessful(self):
    """Tests retries until all subscribers are successful."""
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    # Simulate that callback 2 is successful and the rest fail.
    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=2)
    event.update(more, sub_list[:1])
    event = EventToDeliver.get(event.key())
    self.assertTrue(more)
    self.assertEquals(self.callback3, event.last_callback)
    self.assertEquals(EventToDeliver.NORMAL, event.delivery_mode)

    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=2)
    event.update(more, sub_list[2:])
    event = EventToDeliver.get(event.key())
    self.assertEquals('', event.last_callback)
    self.assertFalse(more)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)

    # Now getting the next subscribers will returned the failed ones.
    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=2)
    expected = sub_keys[:1] + sub_keys[2:3]
    self.assertEquals(expected, [s.key() for s in subs])
    event.update(more, subs)
    event = EventToDeliver.get(event.key())
    self.assertTrue(more)
    self.assertEquals(self.callback, event.last_callback)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)

    # This will get the last of the failed subscribers but *not* include the
    # sentinel value of event.last_callback, since that marks the end of this
    # attempt.
    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=2)
    expected = sub_keys[3:]
    self.assertEquals(expected, [s.key() for s in subs])
    event.update(more, subs)
    event = EventToDeliver.get(event.key())
    self.assertFalse(more)
    self.assertEquals('', event.last_callback)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)
    self.assertEquals(sub_keys[:1] + sub_keys[2:], event.failed_callbacks)

    # Now simulate all retries being successful one chunk at a time.
    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=2)
    expected = sub_keys[:1] + sub_keys[2:3]
    self.assertEquals(expected, [s.key() for s in subs])
    event.update(more, [])
    event = EventToDeliver.get(event.key())
    self.assertTrue(more)
    self.assertEquals(self.callback, event.last_callback)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)
    self.assertEquals(sub_keys[3:], event.failed_callbacks)

    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=2)
    expected = sub_keys[3:]
    self.assertEquals(expected, [s.key() for s in subs])
    event.update(more, [])
    self.assertFalse(more)
    self.assertTrue(EventToDeliver.get(work_key) is None)

  def testGetNextSubscribers_failedFewerThanChunkSize(self):
    """Tests when there are fewer failed callbacks than the chunk size.

    Ensures that we step through retry attempts when there is only a single
    chunk to go through on each retry iteration.
    """
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    # Simulate that callback 2 is successful and the rest fail.
    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=2)
    event.update(more, sub_list[:1])
    event = EventToDeliver.get(event.key())
    self.assertTrue(more)
    self.assertEquals(self.callback3, event.last_callback)
    self.assertEquals(EventToDeliver.NORMAL, event.delivery_mode)

    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=2)
    event.update(more, sub_list[2:])
    event = EventToDeliver.get(event.key())
    self.assertEquals('', event.last_callback)
    self.assertFalse(more)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)
    self.assertEquals(1, event.retry_attempts)

    # Now attempt a retry with a chunk size equal to the number of callbacks.
    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=3)
    event.update(more, subs)
    event = EventToDeliver.get(event.key())
    self.assertFalse(more)
    self.assertEquals(2, event.retry_attempts)

  def testGetNextSubscribers_giveUp(self):
    """Tests retry delay amounts until we finally give up on event delivery.
    
    Verifies retry delay logic works properly.
    """
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    start = datetime.datetime.utcnow()
    def now():
      return start

    self.assertTrue(EventToDeliver.get_work(now=now) is not None)
    memcache.delete(str(event.key()))

    for i, delay in enumerate((5, 10, 20, 40, 80, 160, 320, 640)):
      self.assertTrue(memcache.set(work_key, 'owned'))
      more, subs = event.get_next_subscribers(chunk_size=4)
      event.update(more, subs, retry_period=5, now=now)
      event = EventToDeliver.get(event.key())
      self.assertEquals(i+1, event.retry_attempts)
      self.assertEquals(event.last_modified,
                        start + datetime.timedelta(seconds=delay))
      self.assertFalse(event.totally_failed)
      # Getting work will yield no events here, since the ETA is
      # far into the future.
      self.assertTrue(EventToDeliver.get_work(now=now) is None)

    self.assertTrue(memcache.set(work_key, 'owned'))
    more, subs = event.get_next_subscribers(chunk_size=4)
    event.update(more, subs)
    event = EventToDeliver.get(event.key())
    self.assertTrue(event.totally_failed)

  def testGetWork(self):
    event1 = EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries).put()
    event2 = EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries).put()
    work1 = EventToDeliver.get_work()
    work2 = EventToDeliver.get_work()
    self.assertNotEquals(work1.key(), work2.key())
    work3 = EventToDeliver.get_work()
    self.assertTrue(work3 is None)

################################################################################

class PublishHandlerTest(testutil.HandlerTestBase):

  handler_class = main.PublishHandler

  def testDebugFormRenders(self):
    self.handle('get')
    self.assertTrue('<html>' in self.response_body())

  def testBadMode(self):
    self.handle('post',
                ('hub.mode', 'invalid'),
                ('hub.url', 'http://example.com'))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.mode' in self.response_body())
  
  def testNoUrls(self):
    self.handle('post', ('hub.mode', 'publish'))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.url' in self.response_body())

  def testBadUrls(self):
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', 'http://example.com/bad_url#fragment'))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.url invalid' in self.response_body())

  def testInsertion(self):
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/second-url'),
                ('hub.url', 'http://example.com/third-url'))
    self.assertEquals(204, self.response_code())
    work1 = FeedToFetch.get_work()
    work2 = FeedToFetch.get_work()
    work3 = FeedToFetch.get_work()
    inserted_urls = set(w.topic for w in (work1, work2, work3))
    expected_urls = set(['http://example.com/first-url',
                         'http://example.com/second-url',
                         'http://example.com/third-url'])
    self.assertEquals(expected_urls, inserted_urls)
    self.assertTrue(FeedToFetch.get_work() is None)

  def testDuplicateUrls(self):
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/second-url'),
                ('hub.url', 'http://example.com/second-url'),
                ('hub.url', 'http://example.com/second-url'),
                ('hub.url', 'http://example.com/second-url'),
                ('hub.url', 'http://example.com/second-url'),
                ('hub.url', 'http://example.com/second-url'),
                ('hub.url', 'http://example.com/second-url'))
    self.assertEquals(204, self.response_code())
    work1 = FeedToFetch.get_work()
    work2 = FeedToFetch.get_work()
    inserted_urls = set(w.topic for w in (work1, work2))
    expected_urls = set(['http://example.com/first-url',
                         'http://example.com/second-url'])
    self.assertEquals(expected_urls, inserted_urls)
    self.assertTrue(FeedToFetch.get_work() is None)

  def testInsertFailure(self):
    """Tests when a publish event fails insertion."""
    old_insert = FeedToFetch.insert
    try:
      for exception in (db.Error(), apiproxy_errors.Error(),
                        runtime.DeadlineExceededError()):
        @classmethod
        def new_insert(cls, *args):
          raise exception
        FeedToFetch.insert = new_insert
        self.handle('post',
                    ('hub.mode', 'PuBLisH'),
                    ('hub.url', 'http://example.com/first-url'),
                    ('hub.url', 'http://example.com/second-url'),
                    ('hub.url', 'http://example.com/third-url'))
        self.assertEquals(503, self.response_code())
    finally:
      FeedToFetch.insert = old_insert


class PublishHandlerThroughHubUrlTest(PublishHandlerTest):

  handler_class = main.HubHandler

################################################################################

class FindFeedUpdatesTest(unittest.TestCase):

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.topic = 'http://example.com/my-topic-here'
    self.header_footer = '<feed>this is my test header footer</feed>'
    self.entries_map = {
        'id1': ('time1', 'content1'),
        'id2': ('time2', 'content2'),
        'id3': ('time3', 'content3'),
    }
    self.content = 'the expected response data'
    def my_filter(ignored, content):
      self.assertEquals(self.content, content)
      return self.header_footer, self.entries_map
    self.my_filter = my_filter

  def run_test(self):
    """Runs a test."""
    header_footer, entry_list = main.find_feed_updates(
        self.topic, self.content, filter_feed=self.my_filter)
    self.assertEquals(self.header_footer, header_footer)
    return entry_list

  @staticmethod
  def get_entry(entry_id, entry_list):
    """Finds the entry with the given ID in the list of entries."""
    return [e for e in entry_list if e.entry_id == entry_id][0]

  def testAllNewContent(self):
    """Tests when al pulled feed content is new."""
    entry_list = self.run_test()
    entry_id_set = set(f.entry_id for f in entry_list)
    self.assertEquals(set(self.entries_map.keys()), entry_id_set)

  def testSomeExistingEntries(self):
    """Tests when some entries are already known."""
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id1', 'time1', 'oldcontent1').put()
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id2', 'time2', 'oldcontent2').put()
    
    entry_list = self.run_test()
    entry_id_set = set(f.entry_id for f in entry_list)
    self.assertEquals(set(['id3']), entry_id_set)

  def testPulledEntryNewer(self):
    """Tests when an entry is already known but has been updated recently."""
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id1', 'time1', 'oldcontent1').put()
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id2', 'time2', 'oldcontent2').put()
    self.entries_map['id1'] = ('time4', 'newcontent1')

    entry_list = self.run_test()
    entry_id_set = set(f.entry_id for f in entry_list)
    self.assertEquals(set(['id1', 'id3']), entry_id_set)

    # Verify the old entry would be overwritten.
    entry1 = self.get_entry('id1', entry_list)
    self.assertEquals('newcontent1', entry1.entry_payload)
    self.assertEquals('time4', entry1.entry_updated)

################################################################################

FeedRecord = main.FeedRecord


class PullFeedHandlerTest(testutil.HandlerTestBase):

  def setUp(self):
    """Sets up the test harness."""
    self.topic = 'http://example.com/my-topic-here'
    self.header_footer = '<feed>this is my test header footer</feed>'
    self.all_ids = ['1', '2', '3']
    self.entry_list = [
        FeedEntryRecord.create_entry_for_topic(
            self.topic, entry_id, 'update_time', 'content%s' % entry_id)
        for entry_id in self.all_ids
    ]
    self.expected_response = 'the expected response data'
    self.etag = 'something unique'
    self.last_modified = 'some time'
    self.headers = {
      'ETag': self.etag,
      'Last-Modified': self.last_modified,
      'Content-Type': 'application/atom+xml',
    }

    def my_find_updates(ignored, content):
      self.assertEquals(self.expected_response, content)
      return self.header_footer, self.entry_list
    def create_handler():
      return main.PullFeedHandler(find_feed_updates=my_find_updates)
    self.handler_class = create_handler
    testutil.HandlerTestBase.setUp(self)
    self.callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(self.callback, self.topic))

  def tearDown(self):
    """Tears down the test harness."""
    urlfetch_test_stub.instance.verify_and_reset()

  def testNoWork(self):
    self.handle('get')

  def testNewEntries(self):
    """Tests when new entries are found."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.handle('get')

    # Verify that all feed entry records have been written along with the
    # EventToDeliver and FeedRecord.
    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.all_ids)
    self.assertEquals(self.all_ids, [e.entry_id for e in feed_entries])

    work = EventToDeliver.get_work()
    self.assertEquals(self.topic, work.topic)
    self.assertTrue('content1\ncontent2\ncontent3' in work.payload)
    work.delete()

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals(self.header_footer, record.header_footer)
    self.assertEquals(self.etag, record.etag)
    self.assertEquals(self.last_modified, record.last_modified)
    self.assertEquals('application/atom+xml', record.content_type)

  def testCacheHit(self):
    """Tests when the fetched feed matches the last cached version of it."""
    info = FeedRecord.get_or_create(self.topic)
    info.update(self.headers)
    info.put()

    request_headers = {
      'If-None-Match': self.etag,
      'If-Modified-Since': self.last_modified,
    }

    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 304, '',
        request_headers=request_headers,
        response_headers=self.headers)
    self.handle('get')

  def testNoNewEntries(self):
    """Tests when there are no new entries."""
    FeedToFetch.insert([self.topic])
    self.entry_list = []
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.handle('get')

    # There will be no event to deliver, but the feed records will be written.
    self.assertTrue(EventToDeliver.get_work() is None)

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals(self.header_footer, record.header_footer)
    self.assertEquals(self.etag, record.etag)
    self.assertEquals(self.last_modified, record.last_modified)
    self.assertEquals('application/atom+xml', record.content_type)

  def testPullError(self):
    """Tests when URLFetch raises an exception."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response, urlfetch_error=True)
    self.handle('get')
    feed = FeedToFetch.get_by_key_name(main.get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)

  def testPullBadStatusCode(self):
    """Tests when the response status is bad."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 500, self.expected_response)
    self.handle('get')
    feed = FeedToFetch.get_by_key_name(main.get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)

  def testApiProxyError(self):
    """Tests when the APIProxy raises an error."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response, apiproxy_error=True)
    self.handle('get')
    feed = FeedToFetch.get_by_key_name(main.get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)

  def testNoSubscribers(self):
    """Tests that when a feed has no subscribers we do not pull it."""
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    db.put(KnownFeed.create(self.topic))
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)
    self.entry_list = []
    FeedToFetch.insert([self.topic])
    self.handle('get')

    # Verify that *no* feed entry records have been written.
    self.assertEquals([], FeedEntryRecord.get_entries_for_topic(
                               self.topic, self.all_ids))

    # And any KnownFeeds were deleted.
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)


class PullFeedHandlerTestWithParsing(testutil.HandlerTestBase):
  
  handler_class = main.PullFeedHandler

  def testPullBadContent(self):
    """Tests when the content doesn't parse correctly."""
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect(
        'get', topic, 200, 'this does not parse')
    self.handle('get')
    feed = FeedToFetch.get_by_key_name(main.get_hash_key_name(topic))
    self.assertEquals(1, feed.fetching_failures)
  
  def testPullBadFeed(self):
    """Tests when the content parses, but is not a good Atom document."""
    data = ('<?xml version="1.0" encoding="utf-8"?>\n'
            '<meep><entry>wooh</entry></meep>')
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect('get', topic, 200, data)
    self.handle('get')
    feed = FeedToFetch.get_by_key_name(main.get_hash_key_name(topic))
    self.assertEquals(1, feed.fetching_failures)
  
  def testPullGoodContent(self):
    """Tests when the XML can parse just fine."""
    data = ('<?xml version="1.0" encoding="utf-8"?>\n<feed><my header="data"/>'
            '<entry><id>1</id><updated>123</updated>wooh</entry></feed>')
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect('get', topic, 200, data)
    self.handle('get')
    feed = FeedToFetch.get_by_key_name(main.get_hash_key_name(topic))
    self.assertTrue(feed is None)

################################################################################

class PushEventHandlerTest(testutil.HandlerTestBase):

  def setUp(self):
    """Sets up the test harness."""
    self.now = [datetime.datetime.utcnow() + datetime.timedelta(seconds=120)]
    def create_handler():
      return main.PushEventHandler(now=lambda: self.now[0])
    self.handler_class = create_handler
    testutil.HandlerTestBase.setUp(self)

    self.chunk_size = main.EVENT_SUBSCRIBER_CHUNK_SIZE
    self.topic = 'http://example.com/hamster-topic'
    # Order of these URL fetches is determined by the ordering of the hashes
    # of the callback URLs, so we need random extra strings here to get
    # alphabetical hash order.
    self.callback1 = 'http://example.com/hamster-callback1'
    self.callback2 = 'http://example.com/hamster-callback2'
    self.callback3 = 'http://example.com/hamster-callback3-12345'
    self.callback4 = 'http://example.com/hamster-callback4-12345'
    self.header_footer = '<feed>\n<stuff>blah</stuff>\n<xmldata/></feed>'
    self.test_entries = [
        FeedEntryRecord.create_entry_for_topic(self.topic, '1', 'time1',
                                               '<entry>article1</entry>'),
        FeedEntryRecord.create_entry_for_topic(self.topic, '2', 'time2',
                                               '<entry>article2</entry>'),
        FeedEntryRecord.create_entry_for_topic(self.topic, '3', 'time3',
                                               '<entry>article3</entry>'),
    ]
    self.expected_payload = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<feed>\n'
        '<stuff>blah</stuff>\n'
        '<xmldata/>\n'
        '<entry>article1</entry>\n'
        '<entry>article2</entry>\n'
        '<entry>article3</entry>\n'
        '</feed>'
    )

  def tearDown(self):
    """Resets any external modules modified for testing."""
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = self.chunk_size
    urlfetch_test_stub.instance.verify_and_reset()

  def testNoWork(self):
    self.handle('get')
  
  def testNoExtraSubscribers(self):
    """Tests when a single chunk of delivery is enough."""
    self.assertTrue(Subscription.insert(self.callback1, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 200, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 204, '', request_payload=self.expected_payload)
    EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries).put()
    self.handle('get')
    self.assertTrue(EventToDeliver.get_work() is None)

  def testExtraSubscribers(self):
    """Tests when there are more subscribers to contact after delivery."""
    self.assertTrue(Subscription.insert(self.callback1, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 1
    EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries).put()

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 200, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()
    
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 204, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()
    self.assertTrue(EventToDeliver.get_work() is None)

  def testBrokenCallbacks(self):
    """Tests that when callbacks return errors and are saved for later."""
    self.assertTrue(Subscription.insert(self.callback1, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 2
    EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries).put()

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 404, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 500, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    # Force work retrieval to see the status of the event.
    work = EventToDeliver.get_work(
        now=lambda: datetime.datetime.now() + datetime.timedelta(hours=1))
    sub_list = Subscription.get(work.failed_callbacks)
    callback_list = [sub.callback for sub in sub_list]

    self.assertEquals([self.callback1, self.callback2, self.callback3],
                      callback_list)

  def testDeadlineError(self):
    """Tests that callbacks in flight at deadline will be marked as failed."""
    try:
      def deadline():
        raise runtime.DeadlineExceededError()
      main.async_proxy.wait = deadline
      
      self.assertTrue(Subscription.insert(self.callback1, self.topic))
      self.assertTrue(Subscription.insert(self.callback2, self.topic))
      self.assertTrue(Subscription.insert(self.callback3, self.topic))
      main.EVENT_SUBSCRIBER_CHUNK_SIZE = 2
      EventToDeliver.create_event_for_topic(
          self.topic, self.header_footer, self.test_entries).put()
      self.handle('get')

      # All events should be marked as failed even though no urlfetches
      # were made.
      work = EventToDeliver.get_work()
      sub_list = Subscription.get(work.failed_callbacks)
      callback_list = [sub.callback for sub in sub_list]
      self.assertEquals([self.callback1, self.callback2], callback_list)

    finally:
      main.async_proxy = async_apiproxy.AsyncAPIProxy()

  def testRetryLogic(self):
    """Tests that failed urls will be retried after subsequent failures.
    
    This is an end-to-end test for push delivery failures and retries. We'll
    simulate multiple times through the failure list.
    """
    self.assertTrue(Subscription.insert(self.callback1, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    self.assertTrue(Subscription.insert(self.callback4, self.topic))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
    event = EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries)
    event.put()
    event_key = event.key()

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 404, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 204, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 302, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 500, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 404, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 500, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    self.now[0] += datetime.timedelta(seconds=120)
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 200, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    self.now[0] += datetime.timedelta(seconds=120)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 204, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()
    self.assertTrue(db.get(event_key) is None)

################################################################################

class SubscribeHandlerTest(testutil.HandlerTestBase):

  handler_class = main.SubscribeHandler

  def setUp(self):
    """Tests up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.callback = 'http://example.com/good-callback'
    self.topic = 'http://example.com/the-topic'
    self.verify_token = 'the_token'
    self.verify_callback_querystring_template = (
        self.callback +
        '?hub.verify_token=the_token&'
        'hub.topic=http%3A%2F%2Fexample.com%2Fthe-topic'
        '&hub.mode=')

  def testDebugFormRenders(self):
    self.handle('get')
    self.assertTrue('<html>' in self.response_body())
  
  def testValidation(self):
    """Tests form validation."""
    # Bad mode
    self.handle('post',
        ('hub.mode', 'bad'),
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.mode' in self.response_body())

    # Empty callback
    self.handle('post',
        ('hub.mode', 'subscribe'),
        ('hub.callback', ''),
        ('hub.topic', self.topic),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.callback' in self.response_body())

    # Bad callback URL
    self.handle('post',
        ('hub.mode', 'subscribe'),
        ('hub.callback', 'httpf://example.com'),
        ('hub.topic', self.topic),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.callback' in self.response_body())

    # Empty topic
    self.handle('post',
        ('hub.mode', 'subscribe'),
        ('hub.callback', self.callback),
        ('hub.topic', ''),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.topic' in self.response_body())

    # Bad topic URL
    self.handle('post',
        ('hub.mode', 'subscribe'),
        ('hub.callback', self.callback),
        ('hub.topic', 'httpf://example.com'),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.topic' in self.response_body())

    # Bad verify
    self.handle('post',
        ('hub.mode', 'subscribe'),
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.verify', 'async,async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.verify' in self.response_body())

    # Bad verify
    self.handle('post',
        ('hub.mode', 'subscribe'),
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.verify', 'async'),
        ('hub.verify_token', ''))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.verify_token' in self.response_body())
  
  def testUnsubscribeMissingSubscription(self):
    """Tests that deleting a non-existent subscription does nothing."""
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(204, self.response_code())

  def testSynchronous(self):
    """Tests synchronous subscribe and unsubscribe."""
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)

    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe', 204, '')
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(204, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)

    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'unsubscribe', 204, '')
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(204, self.response_code())
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
  
  def testAsynchronous(self):
    """Tests sync and async subscriptions cause the correct state transitions.
    
    Also tests that synchronous subscribes and unsubscribes will overwrite
    asynchronous requests.
    """
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)

    # Async subscription.
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(202, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)

    # Sync subscription overwrites.
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe', 204, '')
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(204, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)

    # Async unsubscribe queues removal.
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(202, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_TO_DELETE, sub.subscription_state)
    
    # Synch unsubscribe overwrites.
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'unsubscribe', 204, '')
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(204, self.response_code())
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
  
  def testResubscribe(self):
    """Tests that subscribe requests will reset pending unsubscribes."""
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)

    # Async subscription.
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(202, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)

    # Async un-subscription.
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(202, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_TO_DELETE, sub.subscription_state)
    
    # Synchronous subscription overwrites.
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe', 204, '')
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(204, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)
  
  def testSynchronousConfirmFailure(self):
    """Tests when synchronous confirmations fail."""
    # Subscribe
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe', 500, '')
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)
    self.assertEquals(409, self.response_code())
    
    # Unsubscribe
    Subscription.insert(self.callback, self.topic)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'unsubscribe', 500, '')
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertTrue(Subscription.get_by_key_name(sub_key) is not None)
    self.assertEquals(409, self.response_code())

  def testAfterSubscriptionError(self):
    """Tests when an exception occurs after subscription."""
    old_confirm = main.ConfirmSubscription
    try:
      for exception in (runtime.DeadlineExceededError(), db.Error(),
                        apiproxy_errors.Error()):
        def new_confirm(*args):
          raise exception
        main.ConfirmSubscription = new_confirm
        self.handle('post',
            ('hub.callback', self.callback),
            ('hub.topic', self.topic),
            ('hub.mode', 'subscribe'),
            ('hub.verify', 'sync'),
            ('hub.verify_token', self.verify_token))
        self.assertEquals(503, self.response_code())
    finally:
      main.ConfirmSubscription = old_confirm
  
  def testSubscriptionError(self):
    """Tests when errors occurs during subscription."""
    # URLFetch errors are probably the subscriber's fault, so we'll serve these
    # as a conflict.
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe',
        None, '', urlfetch_error=True)
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(409, self.response_code())
    
    # An apiproxy error or deadline error will fall through and serve a 503,
    # since that means there's something wrong with our service.
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe',
        None, '', apiproxy_error=True)
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(503, self.response_code())

    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe',
        None, '', deadline_error=True)
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(503, self.response_code())


class SubscribeHandlerThroughHubUrlTest(SubscribeHandlerTest):

  handler_class = main.HubHandler

################################################################################

class SubscriptionConfirmHandlerTest(testutil.HandlerTestBase):

  handler_class = main.SubscriptionConfirmHandler
  
  def setUp(self):
    """Sets up the test fixture."""
    testutil.HandlerTestBase.setUp(self)
    self.callback = 'http://example.com/good-callback'
    self.topic = 'http://example.com/the-topic'
    self.verify_token = 'the_token'
    self.verify_callback_querystring_template = (
        self.callback +
        '?hub.verify_token=the_token&'
        'hub.topic=http%3A%2F%2Fexample.com%2Fthe-topic'
        '&hub.mode=')
  
  def tearDown(self):
    """Verify that all URL fetches occurred."""
    urlfetch_test_stub.instance.verify_and_reset()

  def testNoWork(self):
    self.handle('get')
  
  def testSubscribeSuccessful(self):
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    Subscription.request_insert(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe', 204, '')
    self.handle('get')
    self.assertTrue(Subscription.get_confirm_work() is None)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)

  def testSubscribeFailed(self):
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    Subscription.request_insert(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'subscribe', 500, '')
    self.handle('get')
    sub = Subscription.get_by_key_name(sub_key)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertEquals(1, sub.confirm_failures)

  def testUnsubscribeSuccessful(self):
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    Subscription.insert(self.callback, self.topic)
    Subscription.request_remove(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'unsubscribe', 204, '')
    self.handle('get')
    self.assertTrue(Subscription.get_confirm_work() is None)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)

  def testUnsubscribeFailed(self):
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    Subscription.insert(self.callback, self.topic)
    Subscription.request_remove(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template + 'unsubscribe', 500, '')
    self.handle('get')
    sub = Subscription.get_by_key_name(sub_key)
    self.assertEquals(Subscription.STATE_TO_DELETE, sub.subscription_state)
    self.assertEquals(1, sub.confirm_failures)
  
  def testConfirmError(self):
    """Tests when an exception is raised while confirming a subscription."""
    Subscription.request_insert(self.callback, self.topic, self.verify_token)
    # All exceptions should just fall through.
    old_confirm = main.ConfirmSubscription
    try:
      def new_confirm(*args):
        raise db.Error()
      main.ConfirmSubscription = new_confirm
      try:
        self.handle('get')
      except db.Error:
        pass
      else:
        self.fail()
    finally:
      main.ConfirmSubscription = old_confirm

################################################################################

PollingMarker = main.PollingMarker


class PollBootstrapHandlerTest(testutil.HandlerTestBase):

  handler_class = main.PollBootstrapHandler

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.original_chunk_size = main.BOOSTRAP_FEED_CHUNK_SIZE
    main.BOOSTRAP_FEED_CHUNK_SIZE = 2

  def tearDown(self):
    """Tears down the test harness."""
    testutil.HandlerTestBase.tearDown(self)
    main.BOOSTRAP_FEED_CHUNK_SIZE = self.original_chunk_size

  def testFullFlow(self):
    """Tests a full flow through multiple chunks."""
    topic = 'http://example.com/feed1'
    topic2 = 'http://example.com/feed2'
    topic3 = 'http://example.com/feed3-124'  # alphabetical on the hash of this
    db.put([KnownFeed.create(topic), KnownFeed.create(topic2),
            KnownFeed.create(topic3)])
    self.assertTrue(FeedToFetch.get_by_topic(topic) is None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is None)

    self.handle('get')
    self.assertTrue(FeedToFetch.get_by_topic(topic) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is None)

    self.handle('get')
    self.assertTrue(FeedToFetch.get_by_topic(topic) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is not None)
    self.assertTrue(PollingMarker.get().current_key is not None)

    self.handle('get')  # This completes the cycle.
    the_mark = PollingMarker.get()
    self.assertTrue(the_mark.current_key is None)

    self.handle('get')  # This will do nothing.
    the_mark = PollingMarker.get()
    self.assertTrue(the_mark.current_key is None)
    
    # Resetting the next start time to before the present time will
    # cause the iteration to start again.
    the_mark.next_start = \
        datetime.datetime.utcnow() - datetime.timedelta(seconds=60)
    db.put(the_mark)

    db.delete([FeedToFetch.get_by_topic(t) for t in (topic, topic2, topic3)])
    self.handle('get')
    self.assertTrue(FeedToFetch.get_by_topic(topic) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is None)

################################################################################

if __name__ == '__main__':
  unittest.main()
