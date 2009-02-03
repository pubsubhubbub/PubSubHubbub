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

import logging
logging.basicConfig(format='%(levelname)-8s %(filename)s] %(message)s')
import os
import sys
import unittest

import testutil
testutil.fix_path()

from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import webapp

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
    self.assertTrue(Subscription.request_insert(self.callback, self.topic))
    self.assertFalse(Subscription.request_insert(self.callback, self.topic))
    self.assertEquals(Subscription.STATE_NOT_VERIFIED,
                      self.get_subscription().subscription_state)
  
  def testInsert_defaults(self):
    self.assertTrue(Subscription.insert(self.callback, self.topic))
    self.assertFalse(Subscription.insert(self.callback, self.topic))
    self.assertEquals(Subscription.STATE_VERIFIED,
                      self.get_subscription().subscription_state)

  def testInsert_override(self):
    """Tests that insert will override the existing subscription state."""
    self.assertTrue(Subscription.request_insert(self.callback, self.topic))
    self.assertEquals(Subscription.STATE_NOT_VERIFIED,
                      self.get_subscription().subscription_state)
    self.assertFalse(Subscription.insert(self.callback, self.topic))
    self.assertEquals(Subscription.STATE_VERIFIED,
                      self.get_subscription().subscription_state)
  
  def testRemove(self):
    self.assertFalse(Subscription.remove(self.callback, self.topic))
    self.assertTrue(Subscription.request_insert(self.callback, self.topic))
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    self.assertFalse(Subscription.remove(self.callback, self.topic))
  
  def testRequestRemove(self):
    self.assertFalse(Subscription.request_remove(self.callback, self.topic))
    self.assertTrue(Subscription.request_insert(self.callback, self.topic))
    self.assertTrue(Subscription.request_remove(self.callback, self.topic))
    self.assertEquals(Subscription.STATE_TO_DELETE,
                      self.get_subscription().subscription_state)
    self.assertFalse(Subscription.request_remove(self.callback, self.topic))

  def testHasSubscribers_unverified(self):
    """Tests that unverified subscribers do not make the subscription active."""
    self.assertFalse(Subscription.has_subscribers(self.topic))
    self.assertTrue(Subscription.request_insert(self.callback, self.topic))
    self.assertFalse(Subscription.has_subscribers(self.topic))

  def testHasSubscribers_verified(self):
    self.assertTrue(Subscription.insert(self.callback, self.topic))
    self.assertTrue(Subscription.has_subscribers(self.topic))
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    self.assertFalse(Subscription.has_subscribers(self.topic))
  
  def testGetSubscribers_unverified(self):
    """Tests that unverified subscribers will not be retrieved."""
    self.assertEquals([], Subscription.get_subscribers(self.topic, 10))
    self.assertTrue(Subscription.request_insert(self.callback, self.topic))
    self.assertTrue(Subscription.request_insert(self.callback2, self.topic))
    self.assertTrue(Subscription.request_insert(self.callback3, self.topic))
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
    self.assertTrue(Subscription.request_insert(self.callback, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    self.assertTrue(Subscription.request_remove(self.callback3, self.topic))
    work1 = Subscription.get_confirm_work()
    work2 = Subscription.get_confirm_work()
    self.assertNotEquals(work1.key(), work2.key())
    work3 = Subscription.get_confirm_work()
    self.assertTrue(work3 is None)

################################################################################

FeedEntryRecord = main.FeedEntryRecord
EventToDeliver = main.EventToDeliver


class EventToDeliverTest(unittest.TestCase):
  
  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.topic = 'http://example.com/my-topic'
    self.callback = 'http://example.com/my-callback'
    self.callback2 = 'http://example.com/second-callback'
    self.callback3 = 'http://example.com/third-callback'
    self.header_footer = '<feed>\n<stuff>blah</stuff>\n<xmldata/></feed>'
    self.test_entries = [
        FeedEntryRecord.create_entry_for_topic(self.topic, '1', 'time1',
                                               '<entry>article1</entry>'),
        FeedEntryRecord.create_entry_for_topic(self.topic, '2', 'time2',
                                               '<entry>article2</entry>'),
        FeedEntryRecord.create_entry_for_topic(self.topic, '3', 'time3',
                                               '<entry>article3</entry>'),
    ]

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

  def testUpdate(self):
    event = EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries)
    event.put()
    work_key = str(event.key())
    
    Subscription.insert(self.callback, self.topic)
    Subscription.insert(self.callback2, self.topic)
    Subscription.insert(self.callback3, self.topic)
    sub_list = Subscription.get_subscribers(self.topic, 10)
    self.assertEquals(3, len(sub_list))

    # Assert that the callback offset is updated, the work lock is cleared, and
    # any failed callbacks are recorded.
    self.assertTrue(memcache.set(work_key, 'owned'))
    event.update(True, self.callback, [sub_list[0]])
    event = EventToDeliver.get(event.key())
    self.assertEquals(self.callback, event.last_callback)
    self.assertEquals([sub_list[0].key()], event.failed_callbacks)
    self.assertTrue(memcache.get(work_key) is None)

    self.assertTrue(memcache.set(work_key, 'owned'))
    event.update(True, self.callback2, sub_list[1:])
    event = EventToDeliver.get(event.key())
    self.assertEquals(self.callback2, event.last_callback)
    self.assertEquals([s.key() for s in sub_list],
                      event.failed_callbacks)
    self.assertTrue(memcache.get(work_key) is None)

    # Assert that the final call to update will NOT clear the event because
    # it still has failures that remain.
    event.update(False, '', [])
    self.assertTrue(EventToDeliver.get(work_key) is not None)
  
  def testUpdateWithFailures(self):
    """Tests the update method when callback failures are encountered."""
    # TODO: Tests that update failures are recorded property and eventually
    # cleaned up.

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

FeedToFetch = main.FeedToFetch


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
  
  def testInsertion(self):
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', 'http://example.com/first-url'),
                ('hub.url', 'http://example.com/second-url'),
                ('hub.url', 'http://example.com/third-url'))
    work1 = FeedToFetch.get_work()
    work2 = FeedToFetch.get_work()
    work3 = FeedToFetch.get_work()
    inserted_urls = set(w.topic for w in (work1, work2, work3))
    expected_urls = set(['http://example.com/first-url',
                         'http://example.com/second-url',
                         'http://example.com/third-url'])
    self.assertEquals(expected_urls, inserted_urls)

################################################################################

FeedRecord = main.FeedRecord


class PullFeedHandlerTest(testutil.HandlerTestBase):

  def setUp(self):
    """Sets up the test harness."""    
    self.topic = 'http://example.com/my-topic-here'
    self.header_footer = '<feed>this is my test header footer</feed>'
    self.entries_map = {
        'id1': ('time1', 'content1'),
        'id2': ('time2', 'content2'),
        'id3': ('time3', 'content3'),
    }

    self.expected_response = 'the expected response data'
    def my_filter(ignored, content):
      self.assertEquals(self.expected_response, content)
      return self.header_footer, self.entries_map
    def create_handler():
      return main.PullFeedHandler(filter_feed=my_filter)
    self.handler_class = create_handler    
    testutil.HandlerTestBase.setUp(self)

  def tearDown(self):
    """Tears down the test harness."""
    urlfetch_test_stub.instance.verify_and_reset()

  def testNoWork(self):
    self.handle('get')

  def testAllNewContent(self):
    """Tests when all pulled feed content is new."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response)
    self.handle('get')
    
    # Verify that all feed entry records have been written along with the
    # EventToDeliver and FeedRecord.
    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.entries_map.keys())
    entry_id_set = set(f.entry_id for f in feed_entries)
    self.assertEquals(set(self.entries_map.keys()), entry_id_set)
    
    work = EventToDeliver.get_work()
    self.assertEquals(self.topic, work.topic)
    self.assertTrue('content3\ncontent2\ncontent1' in work.payload)
    work.delete()

    record = FeedRecord.get_by_topic(self.topic)
    self.assertEquals(self.header_footer, record.header_footer)
  
    # Verify that fetching the same feed again will do nothing.
    FeedToFetch.insert([self.topic])
    self.handle('get')
    self.assertTrue(EventToDeliver.get_work() is None)
  
  def testSomeExistingEntries(self):
    """Tests when some entries are already known."""
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id1', 'time1', 'oldcontent1').put()
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id2', 'time2', 'oldcontent2').put()

    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response)
    self.handle('get')
    
    # Verify that the old feed entries were not overwritten.
    entry1 = FeedEntryRecord.get_entries_for_topic(self.topic, ['id1'])[0]
    self.assertEquals('oldcontent1', entry1.entry_payload)
    entry2 = FeedEntryRecord.get_entries_for_topic(self.topic, ['id2'])[0]
    self.assertEquals('oldcontent2', entry2.entry_payload)
    entry3 = FeedEntryRecord.get_entries_for_topic(self.topic, ['id3'])[0]
    self.assertEquals('content3', entry3.entry_payload)
    
    # Verify that the event payload only contains the new one.
    work = EventToDeliver.get_work()
    self.assertTrue('content3' in work.payload)
    self.assertTrue('content2' not in work.payload)
    self.assertTrue('content1' not in work.payload)

  def testPulledEntryNewer(self):
    """Tests when an entry is already known but has been updated recently."""
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id1', 'time1', 'oldcontent1').put()
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id2', 'time2', 'oldcontent2').put()
    self.entries_map['id1'] = ('time4', 'newcontent1')
    
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response)
    self.handle('get')
    
    # Verify the old entry was overwritten.
    entry1 = FeedEntryRecord.get_entries_for_topic(self.topic, ['id1'])[0]
    self.assertEquals('newcontent1', entry1.entry_payload)
    self.assertEquals('time4', entry1.entry_updated)

    # Verify that the event payload contains the new one and the updated one.
    work = EventToDeliver.get_work()
    self.assertTrue('content3' in work.payload)
    self.assertTrue('content2' not in work.payload)
    self.assertTrue('newcontent1' in work.payload)
  
  def testPullError(self):
    """Tests when URLFetch raises an error."""
    # TODO: Write this test.

################################################################################

class PushEventHandlerTest(testutil.HandlerTestBase):

  handler_class = main.PushEventHandler

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.chunk_size = main.EVENT_SUBSCRIBER_CHUNK_SIZE
    self.topic = 'http://example.com/hamster-topic'
    self.callback1 = 'http://example.com/hamster-callback1'
    self.callback2 = 'http://example.com/hamster-callback2'
    self.callback3 = 'http://example.com/hamster-callback3'
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
    
    # Order of these URL fetches is determined by the ordering of the hashes
    # of the callback URLs, so it's kind of random here (which is why 1 and 3
    # come before 2).
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 200, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()
    
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 204, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()
    self.assertTrue(EventToDeliver.get_work() is None)

  def testBrokenCallbacks(self):
    """Tests when callbacks return errors and are saved for later."""
    self.assertTrue(Subscription.insert(self.callback1, self.topic))
    self.assertTrue(Subscription.insert(self.callback2, self.topic))
    self.assertTrue(Subscription.insert(self.callback3, self.topic))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 2
    EventToDeliver.create_event_for_topic(
        self.topic, self.header_footer, self.test_entries).put()

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 404, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()

    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 500, '', request_payload=self.expected_payload)
    self.handle('get')
    urlfetch_test_stub.instance.verify_and_reset()
    
    work = EventToDeliver.get_work()
    sub_list = Subscription.get(work.failed_callbacks)
    callback_list = [sub.callback for sub in sub_list]

    self.assertEquals([self.callback1, self.callback3, self.callback2],
                      callback_list)

################################################################################

if __name__ == '__main__':
  unittest.main()
