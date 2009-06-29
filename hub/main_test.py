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
from google.appengine.api.labs.taskqueue import taskqueue_stub
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.runtime import apiproxy_errors

import async_apiproxy
import feed_diff
import main
import urlfetch_test_stub

################################################################################
# For convenience

sha1_hash = main.sha1_hash
get_hash_key_name = main.get_hash_key_name

FUNNY = '/CaSeSeNsItIvE'

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
                      get_hash_key_name('and now testing a key'))

  def testIsValidUrl(self):
    self.assertTrue(main.is_valid_url(
        'https://example.com:443/path/to?handler=1&b=2'))
    self.assertTrue(main.is_valid_url('http://example.com:8080'))
    self.assertFalse(main.is_valid_url('httpm://example.com'))
    self.assertFalse(main.is_valid_url('http://example.com:9999'))
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

  def testTaskQueueHeader(self):
    os.environ['SERVER_SOFTWARE'] = 'Production'
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = 'Foobar'
    try:
      self.handle('get')
      self.assertEquals('Pass', self.response_body())
    finally:
      del os.environ['HTTP_X_APPENGINE_TASKNAME']

################################################################################

KnownFeed = main.KnownFeed

class KnownFeedTest(unittest.TestCase):
  """Tests for the KnownFeed model class."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.topic = 'http://example.com/my-topic'
    self.topic2 = 'http://example.com/my-topic2'
    self.topic3 = 'http://example.com/my-topic3'

  def testCreateAndDelete(self):
    known_feed = KnownFeed.create(self.topic)
    self.assertEquals(self.topic, known_feed.topic)
    db.put(known_feed)

    found_feed = db.get(KnownFeed.create_key(self.topic))
    self.assertEquals(found_feed.key(), known_feed.key())
    self.assertEquals(found_feed.topic, known_feed.topic)

    db.delete(KnownFeed.create_key(self.topic))
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)

  def testCheckExistsMissing(self):
    self.assertEquals([], KnownFeed.check_exists([]))
    self.assertEquals([], KnownFeed.check_exists([self.topic]))
    self.assertEquals([], KnownFeed.check_exists(
        [self.topic, self.topic2, self.topic3]))
    self.assertEquals([], KnownFeed.check_exists(
        [self.topic, self.topic, self.topic, self.topic2, self.topic2]))

  def testCheckExists(self):
    KnownFeed.create(self.topic).put()
    KnownFeed.create(self.topic2).put()
    KnownFeed.create(self.topic3).put()
    self.assertEquals([self.topic], KnownFeed.check_exists([self.topic]))
    self.assertEquals([self.topic2], KnownFeed.check_exists([self.topic2]))
    self.assertEquals([self.topic3], KnownFeed.check_exists([self.topic3]))
    self.assertEquals(
        sorted([self.topic, self.topic2, self.topic3]),
        sorted(KnownFeed.check_exists([self.topic, self.topic2, self.topic3])))
    self.assertEquals(
        sorted([self.topic, self.topic2]),
        sorted(KnownFeed.check_exists(
            [self.topic, self.topic, self.topic, self.topic2, self.topic2])))

  def testCheckExistsSubset(self):
    KnownFeed.create(self.topic).put()
    KnownFeed.create(self.topic3).put()
    self.assertEquals(
        sorted([self.topic, self.topic3]),
        sorted(KnownFeed.check_exists([self.topic, self.topic2, self.topic3])))
    self.assertEquals(
        sorted([self.topic, self.topic3]),
        sorted(KnownFeed.check_exists(
            [self.topic, self.topic, self.topic,
             self.topic2, self.topic2,
             self.topic3, self.topic3])))

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
    now_datetime = datetime.datetime.now()
    now = lambda: now_datetime
    lease_seconds = 1234

    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, 'token', lease_seconds, now=now))
    self.assertFalse(Subscription.request_insert(
        self.callback, self.topic, 'token', lease_seconds, now=now))

    sub = self.get_subscription()
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertEquals(self.callback, sub.callback)
    self.assertEquals(sha1_hash(self.callback), sub.callback_hash)
    self.assertEquals(self.topic, sub.topic)
    self.assertEquals(sha1_hash(self.topic), sub.topic_hash)
    self.assertEquals(now_datetime + datetime.timedelta(seconds=lease_seconds),
                      sub.expiration_time)
    self.assertEquals(lease_seconds, sub.lease_seconds)

  def testInsert_defaults(self):
    now_datetime = datetime.datetime.now()
    now = lambda: now_datetime
    lease_seconds = 1234

    self.assertTrue(Subscription.insert(self.callback, self.topic,
                                        lease_seconds, now=now))
    self.assertFalse(Subscription.insert(self.callback, self.topic,
                                         lease_seconds, now=now))
    sub = self.get_subscription()
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertEquals(self.callback, sub.callback)
    self.assertEquals(sha1_hash(self.callback), sub.callback_hash)
    self.assertEquals(self.topic, sub.topic)
    self.assertEquals(sha1_hash(self.topic), sub.topic_hash)
    self.assertEquals(now_datetime + datetime.timedelta(seconds=lease_seconds),
                      sub.expiration_time)
    self.assertEquals(lease_seconds, sub.lease_seconds)

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

    key_name1 = Subscription.create_key_name(self.callback, self.topic)
    key_name2 = Subscription.create_key_name(self.callback2, self.topic)
    key_name3 = Subscription.create_key_name(self.callback3, self.topic)

    work1 = Subscription.get_confirm_work(key_name1)
    work3 = Subscription.get_confirm_work(key_name3)
    self.assertNotEquals(work1.key(), work3.key())

    # This won't be retrieved because it has the wrong state.
    self.assertTrue(Subscription.get_confirm_work(key_name2) is None)

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

    for i, delay in enumerate((5, 10, 20, 40, 80)):
      sub.confirm_failed(max_failures=5, retry_period=5, now=now)
      self.assertEquals(sub.eta, start + datetime.timedelta(seconds=delay))
      self.assertEquals(i+1, sub.confirm_failures)

    # It will be deleted on the last try.
    sub.confirm_failed(max_failures=5, retry_period=5)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, index=0, expected_count=6)

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
    FeedToFetch.insert(all_topics)
    found_topics = set(FeedToFetch.get_by_topic(t).topic for t in all_topics)
    self.assertEquals(set(all_topics), found_topics)
    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=3)
    task_topics = set(t['params']['topic'] for t in tasks)
    self.assertEquals(found_topics, task_topics)

  def testEmpty(self):
    """Tests when the list of urls is empty."""
    FeedToFetch.insert([])
    self.assertEquals([], testutil.get_tasks(main.FEED_QUEUE))

  def testDuplicates(self):
    """Tests duplicate urls."""
    all_topics = [self.topic, self.topic, self.topic2, self.topic2]
    FeedToFetch.insert(all_topics)
    found_topics = set(FeedToFetch.get_by_topic(t).topic for t in all_topics)
    self.assertEquals(set(all_topics), found_topics)
    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=2)
    task_topics = set(t['params']['topic'] for t in tasks)
    self.assertEquals(found_topics, task_topics)

  def testDone(self):
    FeedToFetch.insert([self.topic])
    feed = FeedToFetch.get_by_topic(self.topic)
    self.assertTrue(feed.done())
    self.assertTrue(FeedToFetch.get_by_topic(self.topic) is None)

  def testDoneConflict(self):
    """Tests when another entity was written over the top of this one."""
    FeedToFetch.insert([self.topic])
    feed = FeedToFetch.get_by_topic(self.topic)
    FeedToFetch.insert([self.topic])
    self.assertFalse(feed.done())
    self.assertTrue(FeedToFetch.get_by_topic(self.topic) is not None)

  def testFetchFailed(self):
    start = datetime.datetime.utcnow()
    now = lambda: start

    FeedToFetch.insert([self.topic])
    etas = []
    for i, delay in enumerate((5, 10, 20, 40, 80)):
      feed = FeedToFetch.get_by_topic(self.topic)
      feed.fetch_failed(max_failures=5, retry_period=5, now=now)
      expected_eta = start + datetime.timedelta(seconds=delay)
      self.assertEquals(expected_eta, feed.eta)
      etas.append(testutil.task_eta(feed.eta))
      self.assertEquals(i+1, feed.fetching_failures)
      self.assertEquals(False, feed.totally_failed)

    feed.fetch_failed(max_failures=5, retry_period=5, now=now)
    self.assertEquals(True, feed.totally_failed)

    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=6)
    found_etas = [t['eta'] for t in tasks[1:]]  # First task is from insert()
    self.assertEquals(etas, found_etas)

  def testQueuePreserved(self):
    """Tests the request's queue is preserved for inserted FeedToFetchs."""
    FeedToFetch.insert([self.topic])
    feed = FeedToFetch.all().get()
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    feed.delete()

    os.environ['X_APPENGINE_QUEUENAME'] = main.POLLING_QUEUE
    try:
      FeedToFetch.insert([self.topic])
      feed = FeedToFetch.all().get()
      testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
      testutil.get_tasks(main.POLLING_QUEUE, expected_count=1)
    finally:
      del os.environ['X_APPENGINE_QUEUENAME']

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
    self.test_payloads = [
        '<entry>article1</entry>',
        '<entry>article2</entry>',
        '<entry>article3</entry>',
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
        self.topic, main.ATOM, self.header_footer, self.test_payloads)
    event.put()
    work_key = event.key()

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
        self.topic, main.ATOM, self.header_footer, self.test_payloads)
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

  def testCreateEventForTopic_Rss(self):
    """Tests that the RSS payload is properly formed."""
    self.test_payloads = [
        '<item>article1</item>',
        '<item>article2</item>',
        '<item>article3</item>',
    ]
    self.header_footer = (
        '<rss>\n<channel>\n<stuff>blah</stuff>\n<xmldata/></channel>\n</rss>')
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.RSS, self.header_footer, self.test_payloads)
    expected_data = \
u"""<?xml version="1.0" encoding="utf-8"?>
<rss>
<channel>
<stuff>blah</stuff>
<xmldata/>
<item>article1</item>
<item>article2</item>
<item>article3</item>
</channel>
</rss>"""
    self.assertEquals(expected_data, event.payload)

  def testCreateEvent_badHeaderFooter(self):
    """Tests when the header/footer data in an event is invalid."""
    self.assertRaises(AssertionError, EventToDeliver.create_event_for_topic,
        self.topic, main.ATOM, '<feed>has no end tag', self.test_payloads)

  def testNormal_noFailures(self):
    """Tests that event delivery with no failures will delete the event."""
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()
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

    # Assert that the callback offset is updated and any failed callbacks
    # are recorded.
    more, subs = event.get_next_subscribers(chunk_size=1)
    event.update(more, [sub_list[0]])
    event = EventToDeliver.get(event.key())
    self.assertEquals(EventToDeliver.NORMAL, event.delivery_mode)
    self.assertEquals([sub_list[0].key()], event.failed_callbacks)
    self.assertEquals(self.callback2, event.last_callback)

    more, subs = event.get_next_subscribers(chunk_size=3)
    event.update(more, sub_list[1:])
    event = EventToDeliver.get(event.key())
    self.assertTrue(event is not None)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)
    self.assertEquals('', event.last_callback)

    self.assertEquals([s.key() for s in sub_list], event.failed_callbacks)
    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=2)
    self.assertEquals([str(work_key)] * 2,
                      [t['params']['event_key'] for t in tasks])

  def testUpdate_actuallyNoMoreCallbacks(self):
    """Tests when the normal update delivery has no Subscriptions left.

    This tests the case where update is called with no Subscribers in the
    list of Subscriptions. This can happen if a Subscription is deleted
    between when an update happens and when the work queue is invoked again.
    """
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    more, subs = event.get_next_subscribers(chunk_size=3)
    event.update(more, subs)
    event = EventToDeliver.get(event.key())
    self.assertEquals(self.callback4, event.last_callback)
    self.assertEquals(EventToDeliver.NORMAL, event.delivery_mode)

    # This final call to update will transition to retry properly.
    Subscription.remove(self.callback4, self.topic)
    more, subs = event.get_next_subscribers(chunk_size=1)
    event.update(more, [])
    event = EventToDeliver.get(event.key())
    self.assertEquals([], subs)
    self.assertTrue(event is not None)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=2)
    self.assertEquals([str(work_key)] * 2,
                      [t['params']['event_key'] for t in tasks])

  def testGetNextSubscribers_retriesFinallySuccessful(self):
    """Tests retries until all subscribers are successful."""
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    # Simulate that callback 2 is successful and the rest fail.
    more, subs = event.get_next_subscribers(chunk_size=2)
    event.update(more, sub_list[:1])
    event = EventToDeliver.get(event.key())
    self.assertTrue(more)
    self.assertEquals(self.callback3, event.last_callback)
    self.assertEquals(EventToDeliver.NORMAL, event.delivery_mode)

    more, subs = event.get_next_subscribers(chunk_size=2)
    event.update(more, sub_list[2:])
    event = EventToDeliver.get(event.key())
    self.assertEquals('', event.last_callback)
    self.assertFalse(more)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)

    # Now getting the next subscribers will returned the failed ones.
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
    more, subs = event.get_next_subscribers(chunk_size=2)
    expected = sub_keys[:1] + sub_keys[2:3]
    self.assertEquals(expected, [s.key() for s in subs])
    event.update(more, [])
    event = EventToDeliver.get(event.key())
    self.assertTrue(more)
    self.assertEquals(self.callback, event.last_callback)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)
    self.assertEquals(sub_keys[3:], event.failed_callbacks)

    more, subs = event.get_next_subscribers(chunk_size=2)
    expected = sub_keys[3:]
    self.assertEquals(expected, [s.key() for s in subs])
    event.update(more, [])
    self.assertFalse(more)

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=5)
    self.assertEquals([str(work_key)] * 5,
                      [t['params']['event_key'] for t in tasks])

  def testGetNextSubscribers_failedFewerThanChunkSize(self):
    """Tests when there are fewer failed callbacks than the chunk size.

    Ensures that we step through retry attempts when there is only a single
    chunk to go through on each retry iteration.
    """
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    # Simulate that callback 2 is successful and the rest fail.
    more, subs = event.get_next_subscribers(chunk_size=2)
    event.update(more, sub_list[:1])
    event = EventToDeliver.get(event.key())
    self.assertTrue(more)
    self.assertEquals(self.callback3, event.last_callback)
    self.assertEquals(EventToDeliver.NORMAL, event.delivery_mode)

    more, subs = event.get_next_subscribers(chunk_size=2)
    event.update(more, sub_list[2:])
    event = EventToDeliver.get(event.key())
    self.assertEquals('', event.last_callback)
    self.assertFalse(more)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)
    self.assertEquals(1, event.retry_attempts)

    # Now attempt a retry with a chunk size equal to the number of callbacks.
    more, subs = event.get_next_subscribers(chunk_size=3)
    event.update(more, subs)
    event = EventToDeliver.get(event.key())
    self.assertFalse(more)
    self.assertEquals(EventToDeliver.RETRY, event.delivery_mode)
    self.assertEquals(2, event.retry_attempts)

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=3)
    self.assertEquals([str(work_key)] * 3,
                      [t['params']['event_key'] for t in tasks])

  def testGetNextSubscribers_giveUp(self):
    """Tests retry delay amounts until we finally give up on event delivery.

    Verifies retry delay logic works properly.
    """
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()

    start = datetime.datetime.utcnow()
    now = lambda: start

    etas = []
    for i, delay in enumerate((5, 10, 20, 40, 80, 160, 320, 640)):
      more, subs = event.get_next_subscribers(chunk_size=4)
      event.update(more, subs, retry_period=5, now=now)
      event = EventToDeliver.get(event.key())
      self.assertEquals(i+1, event.retry_attempts)
      expected_eta = start + datetime.timedelta(seconds=delay)
      self.assertEquals(expected_eta, event.last_modified)
      etas.append(testutil.task_eta(event.last_modified))
      self.assertFalse(event.totally_failed)

    more, subs = event.get_next_subscribers(chunk_size=4)
    event.update(more, subs)
    event = EventToDeliver.get(event.key())
    self.assertTrue(event.totally_failed)

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=8)
    found_etas = [t['eta'] for t in tasks]
    self.assertEquals(etas, found_etas)

################################################################################

class PublishHandlerTest(testutil.HandlerTestBase):

  handler_class = main.PublishHandler

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    self.topic = 'http://example.com/first-url'
    self.topic2 = 'http://example.com/second-url'
    self.topic3 = 'http://example.com/third-url'

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
    db.put([KnownFeed.create(self.topic),
            KnownFeed.create(self.topic2),
            KnownFeed.create(self.topic3)])
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', self.topic),
                ('hub.url', self.topic2),
                ('hub.url', self.topic3))
    self.assertEquals(204, self.response_code())
    expected_topics = set([self.topic, self.topic2, self.topic3])
    inserted_topics = set(f.topic for f in FeedToFetch.all())
    self.assertEquals(expected_topics, inserted_topics)

  def testIgnoreUnknownFeed(self):
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', self.topic),
                ('hub.url', self.topic2),
                ('hub.url', self.topic3))
    self.assertEquals(204, self.response_code())
    self.assertEquals([], list(FeedToFetch.all()))

  def testDuplicateUrls(self):
    db.put([KnownFeed.create(self.topic),
            KnownFeed.create(self.topic2)])
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', self.topic),
                ('hub.url', self.topic),
                ('hub.url', self.topic),
                ('hub.url', self.topic),
                ('hub.url', self.topic),
                ('hub.url', self.topic),
                ('hub.url', self.topic),
                ('hub.url', self.topic2),
                ('hub.url', self.topic2),
                ('hub.url', self.topic2),
                ('hub.url', self.topic2),
                ('hub.url', self.topic2),
                ('hub.url', self.topic2),
                ('hub.url', self.topic2))
    self.assertEquals(204, self.response_code())
    expected_topics = set([self.topic, self.topic2])
    inserted_topics = set(f.topic for f in FeedToFetch.all())
    self.assertEquals(expected_topics, inserted_topics)

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

  def testCaseSensitive(self):
    """Tests that cases for topics URLs are preserved."""
    self.topic += FUNNY
    self.topic2 += FUNNY
    self.topic3 += FUNNY
    db.put([KnownFeed.create(self.topic),
            KnownFeed.create(self.topic2),
            KnownFeed.create(self.topic3)])
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', self.topic),
                ('hub.url', self.topic2),
                ('hub.url', self.topic3))
    self.assertEquals(204, self.response_code())
    expected_topics = set([self.topic, self.topic2, self.topic3])
    inserted_topics = set(f.topic for f in FeedToFetch.all())
    self.assertEquals(expected_topics, inserted_topics)


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
        'id1': 'content1',
        'id2': 'content2',
        'id3': 'content3',
    }
    self.content = 'the expected response data'
    def my_filter(content, ignored_format):
      self.assertEquals(self.content, content)
      return self.header_footer, self.entries_map
    self.my_filter = my_filter

  def run_test(self):
    """Runs a test."""
    header_footer, entry_list, entry_payloads = main.find_feed_updates(
        self.topic, main.ATOM, self.content, filter_feed=self.my_filter)
    self.assertEquals(self.header_footer, header_footer)
    return entry_list, entry_payloads

  @staticmethod
  def get_entry(entry_id, entry_list):
    """Finds the entry with the given ID in the list of entries."""
    return [e for e in entry_list if e.entry_id == entry_id][0]

  def testAllNewContent(self):
    """Tests when al pulled feed content is new."""
    entry_list, entry_payloads = self.run_test()
    entry_id_set = set(f.entry_id for f in entry_list)
    self.assertEquals(set(self.entries_map.keys()), entry_id_set)
    self.assertEquals(self.entries_map.values(), entry_payloads)

  def testSomeExistingEntries(self):
    """Tests when some entries are already known."""
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id1', sha1_hash('content1')).put()
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id2', sha1_hash('content2')).put()

    entry_list, entry_payloads = self.run_test()
    entry_id_set = set(f.entry_id for f in entry_list)
    self.assertEquals(set(['id3']), entry_id_set)
    self.assertEquals(['content3'], entry_payloads)

  def testPulledEntryNewer(self):
    """Tests when an entry is already known but has been updated recently."""
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id1', sha1_hash('content1')).put()
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id2', sha1_hash('content2')).put()
    self.entries_map['id1'] = 'newcontent1'

    entry_list, entry_payloads = self.run_test()
    entry_id_set = set(f.entry_id for f in entry_list)
    self.assertEquals(set(['id1', 'id3']), entry_id_set)

    # Verify the old entry would be overwritten.
    entry1 = self.get_entry('id1', entry_list)
    self.assertEquals(sha1_hash('newcontent1'), entry1.entry_content_hash)
    self.assertEquals(['content3', 'newcontent1'], entry_payloads)

  def testUnicodeContent(self):
    """Tests when the content contains unicode characters."""
    self.entries_map['id2'] = u'\u2019 asdf'
    entry_list, entry_payloads = self.run_test()
    entry_id_set = set(f.entry_id for f in entry_list)
    self.assertEquals(set(self.entries_map.keys()), entry_id_set)

################################################################################

FeedRecord = main.FeedRecord


class PullFeedHandlerTest(testutil.HandlerTestBase):

  def setUp(self):
    """Sets up the test harness."""
    self.topic = 'http://example.com/my-topic-here'
    self.header_footer = '<feed>this is my test header footer</feed>'
    self.all_ids = ['1', '2', '3']
    self.entry_payloads = [
      'content%s' % entry_id for entry_id in self.all_ids
    ]
    self.entry_list = [
        FeedEntryRecord.create_entry_for_topic(
            self.topic, entry_id, 'content%s' % entry_id)
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
    self.expected_exceptions = []

    def my_find_updates(ignored_topic, ignored_format, content):
      self.assertEquals(self.expected_response, content)
      if self.expected_exceptions:
        raise self.expected_exceptions.pop(0)
      return self.header_footer, self.entry_list, self.entry_payloads

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
    self.handle('post', ('topic', self.topic))

  def testNewEntries_Atom(self):
    """Tests when new entries are found."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.handle('post', ('topic', self.topic))

    # Verify that all feed entry records have been written along with the
    # EventToDeliver and FeedRecord.
    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.all_ids)
    self.assertEquals(self.all_ids, [e.entry_id for e in feed_entries])

    work = EventToDeliver.all().get()
    event_key = work.key()
    self.assertEquals(self.topic, work.topic)
    self.assertTrue('content1\ncontent2\ncontent3' in work.payload)
    work.delete()

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals(self.header_footer, record.header_footer)
    self.assertEquals(self.etag, record.etag)
    self.assertEquals(self.last_modified, record.last_modified)
    self.assertEquals('application/atom+xml', record.content_type)

    task = testutil.get_tasks(main.EVENT_QUEUE, index=0, expected_count=1)
    self.assertEquals(str(event_key), task['params']['event_key'])
    task = testutil.get_tasks(main.FEED_QUEUE, index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])

  def testRssFailBack(self):
    """Tests when parsing as Atom fails and it uses RSS instead."""
    self.expected_exceptions.append(feed_diff.Error('whoops'))
    self.header_footer = '<rss><channel>this is my test</channel></rss>'
    self.headers['Content-Type'] = 'application/xml'

    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.handle('post', ('topic', self.topic))

    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.all_ids)
    self.assertEquals(self.all_ids, [e.entry_id for e in feed_entries])

    work = EventToDeliver.all().get()
    event_key = work.key()
    self.assertEquals(self.topic, work.topic)
    self.assertTrue('content1\ncontent2\ncontent3' in work.payload)
    work.delete()

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals('application/xml', record.content_type)

    task = testutil.get_tasks(main.EVENT_QUEUE, index=0, expected_count=1)
    self.assertEquals(str(event_key), task['params']['event_key'])
    task = testutil.get_tasks(main.FEED_QUEUE, index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])

  def testAtomFailBack(self):
    """Tests when parsing as RSS fails and it uses Atom instead."""
    self.expected_exceptions.append(feed_diff.Error('whoops'))
    self.headers.clear()
    self.headers['Content-Type'] = 'application/rss+xml'
    info = FeedRecord.get_or_create(self.topic)
    info.update(self.headers)
    info.put()

    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.handle('post', ('topic', self.topic))

    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.all_ids)
    self.assertEquals(self.all_ids, [e.entry_id for e in feed_entries])

    work = EventToDeliver.all().get()
    event_key = work.key()
    self.assertEquals(self.topic, work.topic)
    self.assertTrue('content1\ncontent2\ncontent3' in work.payload)
    work.delete()

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals('application/rss+xml', record.content_type)

    task = testutil.get_tasks(main.EVENT_QUEUE, index=0, expected_count=1)
    self.assertEquals(str(event_key), task['params']['event_key'])
    task = testutil.get_tasks(main.FEED_QUEUE, index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])

  def testParseFailure(self):
    """Tests when the feed cannot be parsed as Atom or RSS."""
    self.expected_exceptions.append(feed_diff.Error('whoops'))
    self.expected_exceptions.append(feed_diff.Error('whoops'))
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.handle('post', ('topic', self.topic))

    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)

    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=2)
    self.assertEquals([self.topic] * 2, [t['params']['topic'] for t in tasks])

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
    self.handle('post', ('topic', self.topic))
    self.assertTrue(EventToDeliver.all().get() is None)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

  def testNoNewEntries(self):
    """Tests when there are no new entries."""
    FeedToFetch.insert([self.topic])
    self.entry_list = []
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.handle('post', ('topic', self.topic))
    self.assertTrue(EventToDeliver.all().get() is None)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

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
    self.handle('post', ('topic', self.topic))
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=2)
    self.assertEquals([self.topic] * 2, [t['params']['topic'] for t in tasks])

  def testPullBadStatusCode(self):
    """Tests when the response status is bad."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 500, self.expected_response)
    self.handle('post', ('topic', self.topic))
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=2)
    self.assertEquals([self.topic] * 2, [t['params']['topic'] for t in tasks])

  def testApiProxyError(self):
    """Tests when the APIProxy raises an error."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response, apiproxy_error=True)
    self.handle('post', ('topic', self.topic))
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=2)
    self.assertEquals([self.topic] * 2, [t['params']['topic'] for t in tasks])

  def testNoSubscribers(self):
    """Tests that when a feed has no subscribers we do not pull it."""
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    db.put(KnownFeed.create(self.topic))
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)
    self.entry_list = []
    FeedToFetch.insert([self.topic])
    self.handle('post', ('topic', self.topic))

    # Verify that *no* feed entry records have been written.
    self.assertEquals([], FeedEntryRecord.get_entries_for_topic(
                               self.topic, self.all_ids))

    # And any KnownFeeds were deleted.
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)

    # And there is no EventToDeliver or tasks.
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=1)


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
    self.handle('post', ('topic', topic))
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
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
    self.handle('post', ('topic', topic))
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
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
    self.handle('post', ('topic', topic))
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)

################################################################################

class PushEventHandlerTest(testutil.HandlerTestBase):

  def setUp(self):
    """Sets up the test harness."""
    self.now = [datetime.datetime.utcnow()]
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
    self.test_payloads = [
        '<entry>article1</entry>',
        '<entry>article2</entry>',
        '<entry>article3</entry>',
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
    self.bad_key = db.Key.from_path(EventToDeliver.kind(), 'does_not_exist')

  def tearDown(self):
    """Resets any external modules modified for testing."""
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = self.chunk_size
    urlfetch_test_stub.instance.verify_and_reset()

  def testNoWork(self):
    self.handle('post', ('event_key', str(self.bad_key)))

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
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, self.header_footer, self.test_payloads)
    event.put()
    self.handle('post', ('event_key', str(event.key())))
    self.assertEquals([], list(EventToDeliver.all()))
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

  def testExtraSubscribers(self):
    """Tests when there are more subscribers to contact after delivery."""
    self.assertTrue(Subscription.insert(self.callback1, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 1
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, self.header_footer, self.test_payloads)
    event.put()
    event_key = str(event.key())

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 200, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 204, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()
    self.assertEquals([], list(EventToDeliver.all()))

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=2)
    self.assertEquals([event_key] * 2,
                      [t['params']['event_key'] for t in tasks])

  def testBrokenCallbacks(self):
    """Tests that when callbacks return errors and are saved for later."""
    self.assertTrue(Subscription.insert(self.callback1, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 2
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, self.header_footer, self.test_payloads)
    event.put()
    event_key = str(event.key())

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 404, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 500, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    work = EventToDeliver.all().get()
    sub_list = Subscription.get(work.failed_callbacks)
    callback_list = [sub.callback for sub in sub_list]
    self.assertEquals([self.callback1, self.callback2, self.callback3],
                      callback_list)

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=2)
    self.assertEquals([event_key] * 2,
                      [t['params']['event_key'] for t in tasks])

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
      event = EventToDeliver.create_event_for_topic(
          self.topic, main.ATOM, self.header_footer, self.test_payloads)
      event.put()
      event_key = str(event.key())
      self.handle('post', ('event_key', event_key))

      # All events should be marked as failed even though no urlfetches
      # were made.
      work = EventToDeliver.all().get()
      sub_list = Subscription.get(work.failed_callbacks)
      callback_list = [sub.callback for sub in sub_list]
      self.assertEquals([self.callback1, self.callback2], callback_list)

      self.assertEquals(event_key, testutil.get_tasks(
          main.EVENT_QUEUE, index=0, expected_count=1)['params']['event_key'])
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
        self.topic, main.ATOM, self.header_footer, self.test_payloads)
    event.put()
    event_key = str(event.key())

    # First pass through all URLs goes full speed for two chunks.
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 404, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 204, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 302, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 500, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    # Now the retries.
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 404, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 500, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 200, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 204, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    self.assertEquals([], list(EventToDeliver.all()))
    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=4)
    self.assertEquals([event_key] * 4,
                      [t['params']['event_key'] for t in tasks])

  def testUrlFetchFailure(self):
    """Tests the UrlFetch API raising exceptions while sending notifications."""
    self.assertTrue(Subscription.insert(self.callback1, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, self.header_footer, self.test_payloads)
    event.put()
    event_key = str(event.key())

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 200, '',
        request_payload=self.expected_payload, urlfetch_error=True)
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 200, '',
        request_payload=self.expected_payload, apiproxy_error=True)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    work = EventToDeliver.all().get()
    sub_list = Subscription.get(work.failed_callbacks)
    callback_list = [sub.callback for sub in sub_list]
    self.assertEquals([self.callback1, self.callback2], callback_list)

    self.assertEquals(event_key, testutil.get_tasks(
        main.EVENT_QUEUE, index=0, expected_count=1)['params']['event_key'])

################################################################################

class SubscribeHandlerTest(testutil.HandlerTestBase):

  handler_class = main.SubscribeHandler

  def setUp(self):
    """Tests up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.challenge = 'this_is_my_fake_challenge_string'
    self.old_get_challenge = main.get_random_challenge
    main.get_random_challenge = lambda: self.challenge
    self.callback = 'http://example.com/good-callback'
    self.topic = 'http://example.com/the-topic'
    self.verify_token = 'the_token'
    self.verify_callback_querystring_template = (
        self.callback +
        '?hub.verify_token=the_token'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic'
        '&hub.mode=%s'
        '&hub.lease_seconds=2592000')

  def tearDown(self):
    """Tears down the test harness."""
    testutil.HandlerTestBase.tearDown(self)
    main.get_random_challenge = self.old_get_challenge

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
        ('hub.verify', 'meep'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.verify' in self.response_body())

    # Bad lease_seconds
    self.handle('post',
        ('hub.mode', 'subscribe'),
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.verify', 'async'),
        ('hub.verify_token', 'asdf'),
        ('hub.lease_seconds', 'stuff'))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.lease_seconds' in self.response_body())

    # Bad lease_seconds zero padding will break things
    self.handle('post',
        ('hub.mode', 'subscribe'),
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.verify', 'async'),
        ('hub.verify_token', 'asdf'),
        ('hub.lease_seconds', '000010'))
    self.assertEquals(400, self.response_code())
    self.assertTrue('hub.lease_seconds' in self.response_body())

  def testUnsubscribeMissingSubscription(self):
    """Tests that deleting a non-existent subscription does nothing."""
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.verify', 'sync'),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(204, self.response_code())

  def testSynchronous(self):
    """Tests synchronous subscribe and unsubscribe."""
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)

    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)
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

    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'unsubscribe', 200,
        self.challenge)
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
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)
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
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'unsubscribe', 200,
        self.challenge)
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
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)
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

  def testMaxLeaseSeconds(self):
    """Tests when the max lease period is specified."""
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)

    self.verify_callback_querystring_template = (
        self.callback +
        '?hub.verify_token=the_token'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic'
        '&hub.mode=%s'
        '&hub.lease_seconds=7776000')
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token),
        ('hub.lease_seconds', '1000000000000000000'))
    self.assertEquals(204, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)

  def testInvalidChallenge(self):
    """Tests when the returned challenge is bad."""
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 200, 'bad')
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)
    self.assertEquals(409, self.response_code())

  def testSynchronousConfirmFailure(self):
    """Tests when synchronous confirmations fail."""
    # Subscribe
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 500, '')
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
        self.verify_callback_querystring_template % 'unsubscribe', 500, '')
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
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe',
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
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe',
        None, '', apiproxy_error=True)
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(503, self.response_code())

    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe',
        None, '', deadline_error=True)
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(503, self.response_code())

  def testCaseSensitive(self):
    """Tests that the case of topics, callbacks, and tokens are preserved."""
    self.topic += FUNNY
    self.callback += FUNNY
    self.verify_token += FUNNY
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    self.verify_callback_querystring_template = (
        self.callback +
        '?hub.verify_token=the_token%%2FCaSeSeNsItIvE'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic%%2FCaSeSeNsItIvE'
        '&hub.mode=%s'
        '&hub.lease_seconds=2592000')
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)

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
    self.challenge = 'this_is_my_fake_challenge_string'
    self.old_get_challenge = main.get_random_challenge
    main.get_random_challenge = lambda: self.challenge
    self.sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.verify_token = 'the_token'
    self.verify_callback_querystring_template = (
        self.callback +
        '?hub.verify_token=the_token'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic'
        '&hub.mode=%s'
        '&hub.lease_seconds=2592000')

  def tearDown(self):
    """Verify that all URL fetches occurred."""
    testutil.HandlerTestBase.tearDown(self)
    main.get_random_challenge = self.old_get_challenge
    urlfetch_test_stub.instance.verify_and_reset()

  def verifyTask(self):
    """Verifies that a subscription worker task is present."""
    self.assertEquals(
        self.sub_key,
        testutil.get_tasks(main.SUBSCRIPTION_QUEUE, index=0, expected_count=1)
            ['params']['subscription_key_name'])

  def testNoWork(self):
    self.handle('post', ('subscription_key_name', 'unknown'))

  def testSubscribeSuccessful(self):
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.request_insert(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)
    self.handle('post', ('subscription_key_name', self.sub_key))
    self.verifyTask()
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)

  def testSubscribeFailed(self):
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.request_insert(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 500, '')
    self.handle('post', ('subscription_key_name', self.sub_key))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertEquals(1, sub.confirm_failures)
    self.assertEquals(
        testutil.task_eta(sub.eta),
        testutil.get_tasks(main.SUBSCRIPTION_QUEUE,
                           index=1, expected_count=2)['eta'])

  def testSubscribeBadChallengeResponse(self):
    """Tests when the subscriber responds with a bad challenge."""
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.request_insert(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 200, 'bad')
    self.handle('post', ('subscription_key_name', self.sub_key))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertEquals(1, sub.confirm_failures)
    self.assertEquals(
        testutil.task_eta(sub.eta),
        testutil.get_tasks(main.SUBSCRIPTION_QUEUE,
                           index=1, expected_count=2)['eta'])

  def testUnsubscribeSuccessful(self):
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.insert(self.callback, self.topic)
    Subscription.request_remove(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'unsubscribe', 200,
        self.challenge)
    self.handle('post', ('subscription_key_name', self.sub_key))
    self.verifyTask()
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)

  def testUnsubscribeFailed(self):
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.insert(self.callback, self.topic)
    Subscription.request_remove(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'unsubscribe', 500, '')
    self.handle('post', ('subscription_key_name', self.sub_key))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_TO_DELETE, sub.subscription_state)
    self.assertEquals(1, sub.confirm_failures)
    self.assertEquals(
        testutil.task_eta(sub.eta),
        testutil.get_tasks(main.SUBSCRIPTION_QUEUE,
                           index=1, expected_count=2)['eta'])

  def testConfirmError(self):
    """Tests when an exception is raised while confirming a subscription."""
    called = [False]
    Subscription.request_insert(self.callback, self.topic, self.verify_token)
    # All exceptions should just fall through.
    old_confirm = main.ConfirmSubscription
    try:
      def new_confirm(*args):
        called[0] = True
        raise db.Error()
      main.ConfirmSubscription = new_confirm
      try:
        self.handle('post', ('subscription_key_name', self.sub_key))
      except db.Error:
        pass
      else:
        self.fail()
    finally:
      main.ConfirmSubscription = old_confirm
    self.assertTrue(called[0])

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

    # This will repeatedly insert the initial task to start the polling process.
    self.handle('get')
    self.handle('get')
    self.handle('get')
    task = testutil.get_tasks(main.POLLING_QUEUE, index=0, expected_count=1)
    sequence = task['params']['sequence']

    # Now run the post handler with the params from this first task. It will
    # enqueue another task that starts *after* the last one in the chunk.
    self.handle('post', *task['params'].items())
    self.assertTrue(FeedToFetch.get_by_topic(topic) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is None)

    # Running this handler again will overwrite the FeedToFetch instances,
    # add tasks for them, but it will not duplicate the polling queue Task in
    # the chain of iterating through all KnownFeed entries.
    # TODO(bslatkin): Once the stub's deduping properties are restored, this
    # handler should not enqueue anything.
    self.handle('post', *task['params'].items())
    task = testutil.get_tasks(main.POLLING_QUEUE, index=1, expected_count=3)
    self.assertEquals(
        task, testutil.get_tasks(main.POLLING_QUEUE, index=2, expected_count=3))
    self.assertEquals(sequence, task['params']['sequence'])
    self.assertEquals(str(KnownFeed.create_key(topic2)),
                      task['params']['current_key'])
    self.assertTrue(task['name'].startswith(sequence))

    # Now running another post handler will handle the rest of the feeds.
    self.handle('post', *task['params'].items())
    self.assertTrue(FeedToFetch.get_by_topic(topic) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is not None)

    task = testutil.get_tasks(main.POLLING_QUEUE, index=3, expected_count=4)
    self.assertEquals(sequence, task['params']['sequence'])
    self.assertEquals(str(KnownFeed.create_key(topic3)),
                      task['params']['current_key'])
    self.assertTrue(task['name'].startswith(sequence))

    # Starting the cycle again will do nothing.
    self.handle('get')
    testutil.get_tasks(main.POLLING_QUEUE, expected_count=4)

    # Resetting the next start time to before the present time will
    # cause the iteration to start again.
    the_mark = PollingMarker.get()
    the_mark.next_start = \
        datetime.datetime.utcnow() - datetime.timedelta(seconds=120)
    db.put(the_mark)
    self.handle('get')
    task = testutil.get_tasks(main.POLLING_QUEUE, index=4, expected_count=5)
    self.assertNotEquals(sequence, task['params']['sequence'])

################################################################################

if __name__ == '__main__':
  unittest.main()
