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
import shutil
import sys
import time
import tempfile
import unittest
import urllib
import xml.sax

import testutil
testutil.fix_path()


from google.appengine import runtime
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.runtime import apiproxy_errors

import async_apiproxy
import dos
import feed_diff
import main
import urlfetch_test_stub

import mapreduce.control
import mapreduce.model

################################################################################
# For convenience

sha1_hash = main.sha1_hash
get_hash_key_name = main.get_hash_key_name

OTHER_STRING = '/~one:two/&='
FUNNY = '/CaSeSeNsItIvE'
FUNNY_UNICODE = u'/blah/\u30d6\u30ed\u30b0\u8846'
FUNNY_UTF8 = '/blah/\xe3\x83\x96\xe3\x83\xad\xe3\x82\xb0\xe8\xa1\x86'
FUNNY_IRI = '/blah/%E3%83%96%E3%83%AD%E3%82%B0%E8%A1%86'

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

  def testSha1Hmac(self):
    self.assertEquals('d95abcea4b2a8b0219da7cb04c261639a7bd8c94',
                      main.sha1_hmac('secrat', 'mydatahere'))

  def testIsValidUrl(self):
    self.assertTrue(main.is_valid_url(
        'https://example.com:443/path/to?handler=1&b=2'))
    self.assertTrue(main.is_valid_url('http://example.com:8080'))
    self.assertFalse(main.is_valid_url('httpm://example.com'))
    self.assertFalse(main.is_valid_url('http://example.com:9999'))
    self.assertFalse(main.is_valid_url('http://example.com/blah#bad'))

  def testNormalizeIri(self):
    uri_with_port = u'http://foo.com:9120/url/with/a/port'
    self.assertEquals(uri_with_port, main.normalize_iri(uri_with_port))

    uri_with_query = u'http://foo.com:9120/url?doh=this&port=1'
    self.assertEquals(uri_with_query, main.normalize_iri(uri_with_query))

    uri_with_funny = u'http://foo.com/~myuser/@url!with#nice;delimiter:chars'
    self.assertEquals(uri_with_funny, main.normalize_iri(uri_with_funny))

    not_unicode = 'http://foo.com:9120/url/with/a/port'
    self.assertEquals(not_unicode, main.normalize_iri(not_unicode))

    uri_with_port = u'http://foo.com:9120/url/with/a/port'
    self.assertEquals(uri_with_port, main.normalize_iri(uri_with_port))

    good_iri = (
        'http://www.google.com/reader/public/atom/user'
        '/07256788297315478906/label/%E3%83%96%E3%83%AD%E3%82%B0%E8%A1%86')
    iri = (u'http://www.google.com/reader/public/atom/user'
           u'/07256788297315478906/label/\u30d6\u30ed\u30b0\u8846')
    self.assertEquals(good_iri, main.normalize_iri(iri))

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

  def testRecord(self):
    """Tests the method for recording a feed's identity."""
    KnownFeed.record(self.topic)
    task = testutil.get_tasks(main.MAPPINGS_QUEUE, index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])

################################################################################

KnownFeedIdentity = main.KnownFeedIdentity

class KnownFeedIdentityTest(unittest.TestCase):
  """Tests for the KnownFeedIdentity class."""

  def setUp(self):
    testutil.setup_for_testing()
    self.feed_id = 'my;feed;id'
    self.feed_id2 = 'my;feed;id;2'
    self.topic = 'http://example.com/foobar1'
    self.topic2 = 'http://example.com/meep2'
    self.topic3 = 'http://example.com/stuff3'
    self.topic4 = 'http://example.com/blah4'
    self.topic5 = 'http://example.com/woot5'
    self.topic6 = 'http://example.com/neehaw6'

  def testUpdate(self):
    """Tests the update method."""
    feed = KnownFeedIdentity.update(self.feed_id, self.topic)
    feed_key = KnownFeedIdentity.create_key(self.feed_id)
    self.assertEquals(feed_key, feed.key())
    self.assertEquals(self.feed_id, feed.feed_id)
    self.assertEquals([self.topic], feed.topics)

    feed = KnownFeedIdentity.update(self.feed_id, self.topic2)
    self.assertEquals(self.feed_id, feed.feed_id)
    self.assertEquals([self.topic, self.topic2], feed.topics)

  def testRemove(self):
    """Tests the remove method."""
    # Removing a mapping from an unknown ID does nothing.
    self.assertTrue(KnownFeedIdentity.remove(self.feed_id, self.topic) is None)

    KnownFeedIdentity.update(self.feed_id, self.topic)
    KnownFeedIdentity.update(self.feed_id, self.topic2)

    # Removing an unknown mapping for a known ID does nothing.
    self.assertTrue(KnownFeedIdentity.remove(self.feed_id, self.topic3) is None)

    # Removing from a known ID returns the updated copy.
    feed = KnownFeedIdentity.remove(self.feed_id, self.topic2)
    self.assertEquals([self.topic], feed.topics)

    # Removing a second time does nothing.
    self.assertTrue(KnownFeedIdentity.remove(self.feed_id, self.topic2) is None)
    feed = KnownFeedIdentity.get(KnownFeedIdentity.create_key(self.feed_id))
    self.assertEquals([self.topic], feed.topics)

    # Removing the last one will delete the mapping completely.
    self.assertTrue(KnownFeedIdentity.remove(self.feed_id, self.topic) is None)
    feed = KnownFeedIdentity.get(KnownFeedIdentity.create_key(self.feed_id))
    self.assertTrue(feed is None)

  def testDeriveAdditionalTopics(self):
    """Tests the derive_additional_topics method."""
    # topic, topic2 -> feed_id
    for topic in (self.topic, self.topic2):
      feed = KnownFeed.create(topic)
      feed.feed_id = self.feed_id
      feed.put()
    KnownFeedIdentity.update(self.feed_id, self.topic)
    KnownFeedIdentity.update(self.feed_id, self.topic2)

    # topic3, topic4 -> feed_id2
    for topic in (self.topic3, self.topic4):
      feed = KnownFeed.create(topic)
      feed.feed_id = self.feed_id2
      feed.put()
    KnownFeedIdentity.update(self.feed_id2, self.topic3)
    KnownFeedIdentity.update(self.feed_id2, self.topic4)

    # topic5 -> KnownFeed missing; should not be expanded at all
    # topic6 -> KnownFeed where feed_id = None; default to simple mapping
    KnownFeed.create(self.topic6).put()

    # Put missing topics first to provoke potential ordering errors in the
    # iteration order of the retrieval loop.
    result = KnownFeedIdentity.derive_additional_topics([
        self.topic5, self.topic6, self.topic,
        self.topic2, self.topic3, self.topic4])

    expected = {
      'http://example.com/foobar1':
          set(['http://example.com/foobar1', 'http://example.com/meep2']),
      'http://example.com/meep2':
          set(['http://example.com/foobar1', 'http://example.com/meep2']),
      'http://example.com/blah4':
          set(['http://example.com/blah4', 'http://example.com/stuff3']),
      'http://example.com/neehaw6':
          set(['http://example.com/neehaw6']),
      'http://example.com/stuff3':
          set(['http://example.com/blah4', 'http://example.com/stuff3'])
    }
    self.assertEquals(expected, result)

  def testDeriveAdditionalTopicsWhitespace(self):
    """Tests when the feed ID contains whitespace it is handled correctly.

    This test is only required because the 'feed_identifier' module did not
    properly strip whitespace in its initial version.
    """
    # topic -> feed_id with whitespace
    feed = KnownFeed.create(self.topic)
    feed.feed_id = self.feed_id
    feed.put()
    KnownFeedIdentity.update(self.feed_id, self.topic)

    # topic2 -> feed_id without whitespace
    feed = KnownFeed.create(self.topic2)
    feed.feed_id = '\n  %s  \n \n' % self.feed_id
    feed.put()
    KnownFeedIdentity.update(self.feed_id, self.topic2)

    # topic3 -> KnownFeed where feed_id = all whitespace
    feed = KnownFeed.create(self.topic3)
    feed.feed_id = '\n  \n \n'
    feed.put()

    result = KnownFeedIdentity.derive_additional_topics([
        self.topic, self.topic2, self.topic3])

    expected = {
        'http://example.com/foobar1':
            set(['http://example.com/foobar1', 'http://example.com/meep2']),
        'http://example.com/stuff3':
            set(['http://example.com/stuff3']),
    }
    self.assertEquals(expected, result)

  def testKnownFeedIdentityTooLarge(self):
    """Tests when the fan-out expansion of the KnownFeedIdentity is too big."""
    feed = KnownFeedIdentity.update(self.feed_id, self.topic)
    KnownFeedIdentity.update(
        self.feed_id,
        'http://super-extra-long-topic/' + ('a' * 10000000))
    # Doesn't explode and the update time stays the same.
    new_feed = db.get(feed.key())
    self.assertEquals(feed.last_update, new_feed.last_update)

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
    self.token = 'token'
    self.secret = 'my secrat'
    self.callback_key_map = dict(
        (Subscription.create_key_name(cb, self.topic), cb)
        for cb in (self.callback, self.callback2, self.callback3))

  def get_subscription(self):
    """Returns the subscription for the test callback and topic."""
    return Subscription.get_by_key_name(
        Subscription.create_key_name(self.callback, self.topic))

  def verify_tasks(self, next_state, verify_token, secret, **kwargs):
    """Verifies the required tasks have been submitted.

    Args:
      next_state: The next state the Subscription should have.
      verify_token: The token that should be used to confirm the
        subscription action.
      **kwargs: Passed to testutil.get_tasks().
    """
    task = testutil.get_tasks(main.SUBSCRIPTION_QUEUE, **kwargs)
    self.assertEquals(next_state, task['params']['next_state'])
    self.assertEquals(verify_token, task['params']['verify_token'])
    self.assertEquals(secret, task['params']['secret'])

  def testRequestInsert_defaults(self):
    now_datetime = datetime.datetime.now()
    now = lambda: now_datetime
    lease_seconds = 1234

    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token,
        self.secret, lease_seconds=lease_seconds, now=now))
    self.verify_tasks(Subscription.STATE_VERIFIED, self.token, self.secret,
                      expected_count=1, index=0)
    self.assertFalse(Subscription.request_insert(
        self.callback, self.topic, self.token,
        self.secret, lease_seconds=lease_seconds, now=now))
    self.verify_tasks(Subscription.STATE_VERIFIED, self.token, self.secret,
                      expected_count=2, index=1)

    sub = self.get_subscription()
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertEquals(self.callback, sub.callback)
    self.assertEquals(sha1_hash(self.callback), sub.callback_hash)
    self.assertEquals(self.topic, sub.topic)
    self.assertEquals(sha1_hash(self.topic), sub.topic_hash)
    self.assertEquals(self.token, sub.verify_token)
    self.assertEquals(self.secret, sub.secret)
    self.assertEquals(0, sub.confirm_failures)
    self.assertEquals(now_datetime + datetime.timedelta(seconds=lease_seconds),
                      sub.expiration_time)
    self.assertEquals(lease_seconds, sub.lease_seconds)

  def testInsert_defaults(self):
    now_datetime = datetime.datetime.now()
    now = lambda: now_datetime
    lease_seconds = 1234

    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret,
        lease_seconds=lease_seconds, now=now))
    self.assertFalse(Subscription.insert(
        self.callback, self.topic, self.token, self.secret,
        lease_seconds=lease_seconds, now=now))
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, expected_count=0)

    sub = self.get_subscription()
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertEquals(self.callback, sub.callback)
    self.assertEquals(sha1_hash(self.callback), sub.callback_hash)
    self.assertEquals(self.topic, sub.topic)
    self.assertEquals(sha1_hash(self.topic), sub.topic_hash)
    self.assertEquals(self.token, sub.verify_token)
    self.assertEquals(self.secret, sub.secret)
    self.assertEquals(0, sub.confirm_failures)
    self.assertEquals(now_datetime + datetime.timedelta(seconds=lease_seconds),
                      sub.expiration_time)
    self.assertEquals(lease_seconds, sub.lease_seconds)

  def testInsertOverride(self):
    """Tests that insert will override the existing Subscription fields."""
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertEquals(Subscription.STATE_NOT_VERIFIED,
                      self.get_subscription().subscription_state)

    second_token = 'second token'
    second_secret = 'second secret'
    sub = self.get_subscription()
    sub.confirm_failures = 123
    sub.put()
    self.assertFalse(Subscription.insert(
        self.callback, self.topic, second_token, second_secret))

    sub = self.get_subscription()
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertEquals(0, sub.confirm_failures)
    self.assertEquals(second_token, sub.verify_token)
    self.assertEquals(second_secret, sub.secret)

    self.verify_tasks(Subscription.STATE_VERIFIED, self.token, self.secret,
                      expected_count=1, index=0)

  def testInsert_expiration(self):
    """Tests that the expiration time is updated on repeated insert() calls."""
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret))
    sub = Subscription.all().get()
    expiration1 = sub.expiration_time
    time.sleep(0.5)
    self.assertFalse(Subscription.insert(
        self.callback, self.topic, self.token, self.secret))
    sub = db.get(sub.key())
    expiration2 = sub.expiration_time
    self.assertTrue(expiration2 > expiration1)

  def testRemove(self):
    self.assertFalse(Subscription.remove(self.callback, self.topic))
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    self.assertFalse(Subscription.remove(self.callback, self.topic))
    # Only task should be the initial insertion request.
    self.verify_tasks(Subscription.STATE_VERIFIED, self.token, self.secret,
                      expected_count=1, index=0)

  def testRequestRemove(self):
    """Tests the request remove method."""
    self.assertFalse(Subscription.request_remove(
        self.callback, self.topic, self.token))
    # No tasks should be enqueued and this request should do nothing because
    # no subscription currently exists.
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, expected_count=0)

    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret))
    second_token = 'this is the second token'
    self.assertTrue(Subscription.request_remove(
        self.callback, self.topic, second_token))

    sub = self.get_subscription()
    self.assertEquals(self.token, sub.verify_token)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)

    self.verify_tasks(Subscription.STATE_VERIFIED, self.token, self.secret,
                      expected_count=2, index=0)
    self.verify_tasks(Subscription.STATE_TO_DELETE, second_token, '',
                      expected_count=2, index=1)

  def testRequestInsertOverride(self):
    """Tests that requesting insertion does not override the verify_token."""
    self.assertTrue(Subscription.insert(
         self.callback, self.topic, self.token, self.secret))
    second_token = 'this is the second token'
    second_secret = 'another secret here'
    self.assertFalse(Subscription.request_insert(
        self.callback, self.topic, second_token, second_secret))

    sub = self.get_subscription()
    self.assertEquals(self.token, sub.verify_token)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)

    self.verify_tasks(Subscription.STATE_VERIFIED, second_token, second_secret,
                      expected_count=1, index=0)

  def testHasSubscribers_unverified(self):
    """Tests that unverified subscribers do not make the subscription active."""
    self.assertFalse(Subscription.has_subscribers(self.topic))
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertFalse(Subscription.has_subscribers(self.topic))

  def testHasSubscribers_verified(self):
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.has_subscribers(self.topic))
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    self.assertFalse(Subscription.has_subscribers(self.topic))

  def testGetSubscribers_unverified(self):
    """Tests that unverified subscribers will not be retrieved."""
    self.assertEquals([], Subscription.get_subscribers(self.topic, 10))
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.request_insert(
        self.callback2, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.request_insert(
        self.callback3, self.topic, self.token, self.secret))
    self.assertEquals([], Subscription.get_subscribers(self.topic, 10))

  def testGetSubscribers_verified(self):
    self.assertEquals([], Subscription.get_subscribers(self.topic, 10))
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, self.token, self.secret))
    sub_list = Subscription.get_subscribers(self.topic, 10)
    found_keys = set(s.key().name() for s in sub_list)
    self.assertEquals(set(self.callback_key_map.keys()), found_keys)

  def testGetSubscribers_count(self):
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, self.token, self.secret))
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

    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, self.token, self.secret))

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
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, self.token, self.secret))
    self.assertEquals([], Subscription.get_subscribers(self.topic2, 10))

    self.assertTrue(Subscription.insert(
        self.callback2, self.topic2, self.token, self.secret))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic2, self.token, self.secret))
    sub_list = Subscription.get_subscribers(self.topic2, 10)
    found_keys = set(s.key().name() for s in sub_list)
    self.assertEquals(
        set(Subscription.create_key_name(cb, self.topic2)
            for cb in (self.callback2, self.callback3)),
        found_keys)
    self.assertEquals(3, len(Subscription.get_subscribers(self.topic, 10)))

  def testConfirmFailed(self):
    """Tests retry delay periods when a subscription confirmation fails."""
    start = datetime.datetime.utcnow()
    def now():
      return start

    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret))
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    sub = Subscription.get_by_key_name(sub_key)
    self.assertEquals(0, sub.confirm_failures)

    for i, delay in enumerate((5, 10, 20, 40, 80)):
      self.assertTrue(
          sub.confirm_failed(Subscription.STATE_VERIFIED, self.token, False,
                             max_failures=5, retry_period=5, now=now))
      self.assertEquals(sub.eta, start + datetime.timedelta(seconds=delay))
      self.assertEquals(i+1, sub.confirm_failures)

    # It will give up on the last try.
    self.assertFalse(
        sub.confirm_failed(Subscription.STATE_VERIFIED, self.token, False,
                           max_failures=5, retry_period=5))
    sub = Subscription.get_by_key_name(sub_key)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, index=0, expected_count=6)

  def testQueueSelected(self):
    """Tests that auto_reconfirm will put the task on the polling queue."""
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret,
        auto_reconfirm=True))
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, expected_count=0)
    testutil.get_tasks(main.POLLING_QUEUE, expected_count=1)

    self.assertFalse(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret,
        auto_reconfirm=False))
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, expected_count=1)
    testutil.get_tasks(main.POLLING_QUEUE, expected_count=1)

  def testArchiveExists(self):
    """Tests the archive method when the subscription exists."""
    Subscription.insert(self.callback, self.topic, self.token, self.secret)
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    sub = Subscription.get_by_key_name(sub_key)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    Subscription.archive(self.callback, self.topic)
    sub = Subscription.get_by_key_name(sub_key)
    self.assertEquals(Subscription.STATE_TO_DELETE, sub.subscription_state)

  def testArchiveMissing(self):
    """Tests the archive method when the subscription does not exist."""
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    Subscription.archive(self.callback, self.topic)
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
    found_feeds = FeedToFetch.insert(all_topics)
    task = testutil.get_tasks(main.FEED_QUEUE, index=0, expected_count=1)
    self.assertTrue(task['name'].endswith('%d-0' % found_feeds[0].work_index))

    for topic, feed_to_fetch in zip(all_topics, found_feeds):
      self.assertEquals(topic, feed_to_fetch.topic)
      self.assertEquals([], feed_to_fetch.source_keys)
      self.assertEquals([], feed_to_fetch.source_values)
      self.assertEquals(found_feeds[0].work_index, feed_to_fetch.work_index)

  def testEmpty(self):
    """Tests when the list of urls is empty."""
    FeedToFetch.insert([])
    self.assertEquals([], testutil.get_tasks(main.FEED_QUEUE))

  def testDuplicates(self):
    """Tests duplicate urls."""
    all_topics = [self.topic, self.topic, self.topic2, self.topic2]
    found_feeds = FeedToFetch.insert(all_topics)
    found_topics = set(t.topic for t in found_feeds)
    self.assertEquals(set(all_topics), found_topics)
    task = testutil.get_tasks(main.FEED_QUEUE, index=0, expected_count=1)
    self.assertTrue(task['name'].endswith('%d-0' % found_feeds[0].work_index))

  def testDone(self):
    """Tests marking the feed as completed."""
    (feed,) = FeedToFetch.insert([self.topic])
    self.assertFalse(feed.done())
    self.assertTrue(FeedToFetch.get_by_topic(self.topic) is None)

  def testDoneAfterFailure(self):
    """Tests done() after a fetch_failed() writes the FeedToFetch to disk."""
    (feed,) = FeedToFetch.insert([self.topic])
    feed.fetch_failed()
    self.assertTrue(feed.done())
    self.assertTrue(FeedToFetch.get_by_topic(self.topic) is None)

  def testDoneConflict(self):
    """Tests when another entity was written over the top of this one."""
    (feed1,) = FeedToFetch.insert([self.topic])
    feed1.put()
    (feed2,) = FeedToFetch.insert([self.topic])
    feed2.put()

    self.assertFalse(feed1.done())
    self.assertTrue(FeedToFetch.get_by_topic(self.topic) is not None)

  def testFetchFailed(self):
    """Tests when the fetch fails and should be retried."""
    start = datetime.datetime.utcnow()
    now = lambda: start

    (feed,) = FeedToFetch.insert([self.topic])
    etas = []
    for i, delay in enumerate((5, 10, 20, 40, 80)):
      feed = FeedToFetch.get_by_topic(self.topic) or feed
      feed.fetch_failed(max_failures=5, retry_period=5, now=now)
      expected_eta = start + datetime.timedelta(seconds=delay)
      self.assertEquals(expected_eta, feed.eta)
      etas.append(testutil.task_eta(feed.eta))
      self.assertEquals(i+1, feed.fetching_failures)
      self.assertEquals(False, feed.totally_failed)

    feed.fetch_failed(max_failures=5, retry_period=5, now=now)
    self.assertEquals(True, feed.totally_failed)

    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    tasks.extend(testutil.get_tasks(main.FEED_RETRIES_QUEUE, expected_count=5))
    found_etas = [t['eta'] for t in tasks[1:]]  # First task is from insert()
    self.assertEquals(etas, found_etas)

  def testQueuePreserved(self):
    """Tests the request's polling queue is preserved for new FeedToFetch."""
    FeedToFetch.insert([self.topic])
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)

    os.environ['HTTP_X_APPENGINE_QUEUENAME'] = main.POLLING_QUEUE
    try:
      (feed,) = FeedToFetch.insert([self.topic])
      testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
      testutil.get_tasks(main.POLLING_QUEUE, expected_count=1)
    finally:
      del os.environ['HTTP_X_APPENGINE_QUEUENAME']

  def testSources(self):
    """Tests when sources are supplied."""
    source_dict = {'foo': 'bar', 'meepa': 'stuff'}
    all_topics = [self.topic, self.topic2, self.topic3]
    feed_list = FeedToFetch.insert(all_topics, source_dict=source_dict)
    for feed_to_fetch in feed_list:
      found_source_dict = dict(zip(feed_to_fetch.source_keys,
                                   feed_to_fetch.source_values))
      self.assertEquals(source_dict, found_source_dict)

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
    self.token = 'verify token'
    self.secret = 'some secret'
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
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
    event.put()
    work_key = event.key()

    Subscription.insert(
        self.callback, self.topic, self.token, self.secret)
    Subscription.insert(
        self.callback2, self.topic, self.token, self.secret)
    Subscription.insert(
        self.callback3, self.topic, self.token, self.secret)
    Subscription.insert(
        self.callback4, self.topic, self.token, self.secret)
    sub_list = Subscription.get_subscribers(self.topic, 10)
    sub_keys = [s.key() for s in sub_list]
    self.assertEquals(4, len(sub_list))

    return (event, work_key, sub_list, sub_keys)

  def testCreateEventForTopic(self):
    """Tests that the payload of an event is properly formed."""
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
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
    self.assertEquals('application/atom+xml', event.content_type)

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
        self.topic, main.RSS, 'application/rss+xml',
        self.header_footer, self.test_payloads)
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
    self.assertEquals('application/rss+xml', event.content_type)

  def testCreateEventForTopic_Abitrary(self):
    """Tests that an arbitrary payload is properly formed."""
    self.test_payloads = []
    self.header_footer = 'this is my data here'
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ARBITRARY, 'my crazy content type',
        self.header_footer, self.test_payloads)
    expected_data = 'this is my data here'
    self.assertEquals(expected_data, event.payload)
    self.assertEquals('my crazy content type', event.content_type)

  def testCreateEvent_badHeaderFooter(self):
    """Tests when the header/footer data in an event is invalid."""
    self.assertRaises(AssertionError, EventToDeliver.create_event_for_topic,
        self.topic, main.ATOM, 'content type unused',
        '<feed>has no end tag', self.test_payloads)

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
    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)
    tasks.extend(testutil.get_tasks(main.EVENT_RETRIES_QUEUE, expected_count=1))
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

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)
    tasks.extend(testutil.get_tasks(main.EVENT_RETRIES_QUEUE, expected_count=1))
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

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)
    tasks.extend(testutil.get_tasks(main.EVENT_RETRIES_QUEUE, expected_count=4))
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

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)
    tasks.extend(testutil.get_tasks(main.EVENT_RETRIES_QUEUE, expected_count=2))
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
      event.update(more, subs, retry_period=5, now=now, max_failures=8)
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

    tasks = testutil.get_tasks(main.EVENT_RETRIES_QUEUE, expected_count=8)
    found_etas = [t['eta'] for t in tasks]
    self.assertEquals(etas, found_etas)

  def testQueuePreserved(self):
    """Tests that enqueueing an EventToDeliver preserves the polling queue."""
    event, work_key, sub_list, sub_keys = self.insert_subscriptions()
    def txn():
      event.enqueue()
    db.run_in_transaction(txn)

    testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)
    os.environ['HTTP_X_APPENGINE_QUEUENAME'] = main.POLLING_QUEUE
    try:
      db.run_in_transaction(txn)
    finally:
      del os.environ['HTTP_X_APPENGINE_QUEUENAME']

    testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)
    testutil.get_tasks(main.POLLING_QUEUE, expected_count=1)

  def testMaxFailuresOverride(self):
    """Tests the max_failures override value."""
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
    self.assertEquals(None, event.max_failures)

    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads,
        max_failures=1)
    self.assertEquals(1, event.max_failures)

    Subscription.insert(
        self.callback, self.topic, self.token, self.secret)
    subscription_list = list(Subscription.all())

    event.put()
    event.update(False, subscription_list)
    event2 = db.get(event.key())
    self.assertFalse(event2.totally_failed)

    event2.update(False, [])
    event3 = db.get(event.key())
    self.assertTrue(event3.totally_failed)

################################################################################

class PublishHandlerTest(testutil.HandlerTestBase):

  handler_class = main.PublishHandler

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    self.topic = 'http://example.com/first-url'
    self.topic2 = 'http://example.com/second-url'
    self.topic3 = 'http://example.com/third-url'

  def get_feeds_to_fetch(self):
    """Gets the enqueued FeedToFetch records."""
    return FeedToFetch.FORK_JOIN_QUEUE.pop(
        testutil.get_tasks(main.FEED_QUEUE, index=0, expected_count=1)['name'])

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
    feed_list = self.get_feeds_to_fetch()
    inserted_topics = set(f.topic for f in feed_list)
    self.assertEquals(expected_topics, inserted_topics)

  def testIgnoreUnknownFeed(self):
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', self.topic),
                ('hub.url', self.topic2),
                ('hub.url', self.topic3))
    self.assertEquals(204, self.response_code())
    testutil.get_tasks(main.FEED_QUEUE, expected_count=0)

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
    inserted_topics = set(f.topic for f in self.get_feeds_to_fetch())
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
    inserted_topics = set(f.topic for f in self.get_feeds_to_fetch())
    self.assertEquals(expected_topics, inserted_topics)

  def testNormalization(self):
    """Tests that URLs are properly normalized."""
    self.topic += OTHER_STRING
    self.topic2 += OTHER_STRING
    self.topic3 += OTHER_STRING
    normalized = [
        main.normalize_iri(t)
        for t in [self.topic, self.topic2, self.topic3]]
    db.put([KnownFeed.create(t) for t in normalized])
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', self.topic),
                ('hub.url', self.topic2),
                ('hub.url', self.topic3))
    self.assertEquals(204, self.response_code())
    inserted_topics = set(f.topic for f in self.get_feeds_to_fetch())
    self.assertEquals(set(normalized), inserted_topics)

  def testIri(self):
    """Tests publishing with an IRI with international characters."""
    topic = main.normalize_iri(self.topic + FUNNY_UNICODE)
    topic2 = main.normalize_iri(self.topic2 + FUNNY_UNICODE)
    topic3 = main.normalize_iri(self.topic3 + FUNNY_UNICODE)
    normalized = [topic, topic2, topic3]
    db.put([KnownFeed.create(t) for t in normalized])
    self.handle('post',
                ('hub.mode', 'PuBLisH'),
                ('hub.url', self.topic + FUNNY_UTF8),
                ('hub.url', self.topic2 + FUNNY_UTF8),
                ('hub.url', self.topic3 + FUNNY_UTF8))
    self.assertEquals(204, self.response_code())
    inserted_topics = set(f.topic for f in self.get_feeds_to_fetch())
    self.assertEquals(set(normalized), inserted_topics)

  def testUnicode(self):
    """Tests publishing with a URL that has unicode characters."""
    topic = main.normalize_iri(self.topic + FUNNY_UNICODE)
    topic2 = main.normalize_iri(self.topic2 + FUNNY_UNICODE)
    topic3 = main.normalize_iri(self.topic3 + FUNNY_UNICODE)
    normalized = [topic, topic2, topic3]
    db.put([KnownFeed.create(t) for t in normalized])

    payload = (
        'hub.mode=publish'
        '&hub.url=' + urllib.quote(self.topic) + FUNNY_UTF8 +
        '&hub.url=' + urllib.quote(self.topic2) + FUNNY_UTF8 +
        '&hub.url=' + urllib.quote(self.topic3) + FUNNY_UTF8)
    self.handle_body('post', payload)
    self.assertEquals(204, self.response_code())
    inserted_topics = set(f.topic for f in self.get_feeds_to_fetch())
    self.assertEquals(set(normalized), inserted_topics)

  def testSources(self):
    """Tests that derived sources are properly set on FeedToFetch instances."""
    db.put([KnownFeed.create(self.topic),
            KnownFeed.create(self.topic2),
            KnownFeed.create(self.topic3)])
    source_dict = {'one': 'two', 'three': 'four'}
    topics = [self.topic, self.topic2, self.topic3]
    def derive_sources(handler, urls):
      self.assertEquals(set(topics), set(urls))
      self.assertEquals('testvalue', handler.request.get('the-real-thing'))
      return source_dict

    main.hooks.override_for_test(main.derive_sources, derive_sources)
    try:
      self.handle('post',
                  ('hub.mode', 'PuBLisH'),
                  ('hub.url', self.topic),
                  ('hub.url', self.topic2),
                  ('hub.url', self.topic3),
                  ('the-real-thing', 'testvalue'))
      self.assertEquals(204, self.response_code())
      for feed_to_fetch in self.get_feeds_to_fetch():
        found_source_dict = dict(zip(feed_to_fetch.source_keys,
                                     feed_to_fetch.source_values))
        self.assertEquals(source_dict, found_source_dict)
    finally:
      main.hooks.reset_for_test(main.derive_sources)


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
    return [e for e in entry_list if e.id_hash == sha1_hash(entry_id)][0]

  def testAllNewContent(self):
    """Tests when al pulled feed content is new."""
    entry_list, entry_payloads = self.run_test()
    entry_id_hash_set = set(f.id_hash for f in entry_list)
    self.assertEquals(set(sha1_hash(k) for k in self.entries_map.keys()),
                      entry_id_hash_set)
    self.assertEquals(self.entries_map.values(), entry_payloads)

  def testSomeExistingEntries(self):
    """Tests when some entries are already known."""
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id1', sha1_hash('content1')).put()
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id2', sha1_hash('content2')).put()

    entry_list, entry_payloads = self.run_test()
    entry_id_hash_set = set(f.id_hash for f in entry_list)
    self.assertEquals(set(sha1_hash(k) for k in ['id3']), entry_id_hash_set)
    self.assertEquals(['content3'], entry_payloads)

  def testPulledEntryNewer(self):
    """Tests when an entry is already known but has been updated recently."""
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id1', sha1_hash('content1')).put()
    FeedEntryRecord.create_entry_for_topic(
        self.topic, 'id2', sha1_hash('content2')).put()
    self.entries_map['id1'] = 'newcontent1'

    entry_list, entry_payloads = self.run_test()
    entry_id_hash_set = set(f.id_hash for f in entry_list)
    self.assertEquals(set(sha1_hash(k) for k in ['id1', 'id3']),
                      entry_id_hash_set)

    # Verify the old entry would be overwritten.
    entry1 = self.get_entry('id1', entry_list)
    self.assertEquals(sha1_hash('newcontent1'), entry1.entry_content_hash)
    self.assertEquals(['content3', 'newcontent1'], entry_payloads)

  def testUnicodeContent(self):
    """Tests when the content contains unicode characters."""
    self.entries_map['id2'] = u'\u2019 asdf'
    entry_list, entry_payloads = self.run_test()
    entry_id_hash_set = set(f.id_hash for f in entry_list)
    self.assertEquals(set(sha1_hash(k) for k in self.entries_map.keys()),
                      entry_id_hash_set)

  def testMultipleParallelBatches(self):
    """Tests that retrieving FeedEntryRecords is done in multiple batches."""
    old_get_feed_record = main.FeedEntryRecord.get_entries_for_topic
    calls = [0]
    @staticmethod
    def fake_get_record(*args, **kwargs):
      calls[0] += 1
      return old_get_feed_record(*args, **kwargs)

    old_lookups = main.MAX_FEED_ENTRY_RECORD_LOOKUPS
    main.FeedEntryRecord.get_entries_for_topic = fake_get_record
    main.MAX_FEED_ENTRY_RECORD_LOOKUPS = 1
    try:
      entry_list, entry_payloads = self.run_test()
      entry_id_hash_set = set(f.id_hash for f in entry_list)
      self.assertEquals(set(sha1_hash(k) for k in self.entries_map.keys()),
                        entry_id_hash_set)
      self.assertEquals(self.entries_map.values(), entry_payloads)
      self.assertEquals(3, calls[0])
    finally:
      main.MAX_FEED_ENTRY_RECORD_LOOKUPS = old_lookups
      main.FeedEntryRecord.get_entries_for_topic = old_get_feed_record

################################################################################

FeedRecord = main.FeedRecord


class PullFeedHandlerTest(testutil.HandlerTestBase):

  handler_class = main.PullFeedHandler

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)

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

    self.old_find_feed_updates = main.find_feed_updates
    main.find_feed_updates = my_find_updates

    self.callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, 'token', 'secret'))

  def tearDown(self):
    """Tears down the test harness."""
    main.find_feed_updates = self.old_find_feed_updates
    urlfetch_test_stub.instance.verify_and_reset()

  def run_fetch_task(self, index=0):
    """Runs the currently enqueued fetch task."""
    task = testutil.get_tasks(main.FEED_QUEUE, index=index)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = task['name']
    try:
      self.handle('post')
    finally:
      del os.environ['HTTP_X_APPENGINE_TASKNAME']

  def testNoWork(self):
    self.handle('post', ('topic', self.topic))

  def testNewEntries_Atom(self):
    """Tests when new entries are found."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.run_fetch_task()

    # Verify that all feed entry records have been written along with the
    # EventToDeliver and FeedRecord.
    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.all_ids)
    self.assertEquals(
        [sha1_hash(k) for k in self.all_ids],
        [e.id_hash for e in feed_entries])

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

    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

  def testRssFailBack(self):
    """Tests when parsing as Atom fails and it uses RSS instead."""
    self.expected_exceptions.append(feed_diff.Error('whoops'))
    self.header_footer = '<rss><channel>this is my test</channel></rss>'
    self.headers['Content-Type'] = 'application/xml'

    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.run_fetch_task()

    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.all_ids)
    self.assertEquals(
        [sha1_hash(k) for k in self.all_ids],
        [e.id_hash for e in feed_entries])

    work = EventToDeliver.all().get()
    event_key = work.key()
    self.assertEquals(self.topic, work.topic)
    self.assertTrue('content1\ncontent2\ncontent3' in work.payload)
    work.delete()

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals('application/xml', record.content_type)

    task = testutil.get_tasks(main.EVENT_QUEUE, index=0, expected_count=1)
    self.assertEquals(str(event_key), task['params']['event_key'])

    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

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
    self.run_fetch_task()

    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.all_ids)
    self.assertEquals(
        [sha1_hash(k) for k in self.all_ids],
        [e.id_hash for e in feed_entries])

    work = EventToDeliver.all().get()
    event_key = work.key()
    self.assertEquals(self.topic, work.topic)
    self.assertTrue('content1\ncontent2\ncontent3' in work.payload)
    work.delete()

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals('application/rss+xml', record.content_type)

    task = testutil.get_tasks(main.EVENT_QUEUE, index=0, expected_count=1)
    self.assertEquals(str(event_key), task['params']['event_key'])

    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

  def testArbitraryContent(self):
    """Tests when the feed cannot be parsed as Atom or RSS."""
    self.entry_list = []
    self.entry_payloads = []
    self.header_footer = 'this is all of the content'
    self.expected_exceptions.append(feed_diff.Error('whoops'))
    self.expected_exceptions.append(feed_diff.Error('whoops'))
    FeedToFetch.insert([self.topic])
    self.headers['content-type'] = 'My Crazy Content Type'
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.run_fetch_task()

    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertTrue(feed is None)
    self.assertEquals(0, len(list(FeedEntryRecord.all())))

    work = EventToDeliver.all().get()
    event_key = work.key()
    self.assertEquals(self.topic, work.topic)
    self.assertEquals('this is all of the content', work.payload)
    work.delete()

    record = FeedRecord.get_or_create(self.topic)
    # header_footer not saved for arbitrary data
    self.assertEquals(None, record.header_footer)
    self.assertEquals(self.etag, record.etag)
    self.assertEquals(self.last_modified, record.last_modified)
    self.assertEquals('my crazy content type', record.content_type)

    task = testutil.get_tasks(main.EVENT_QUEUE, index=0, expected_count=1)
    self.assertEquals(str(event_key), task['params']['event_key'])

    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

    testutil.get_tasks(main.FEED_RETRIES_QUEUE, expected_count=0)

    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

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
    self.run_fetch_task()
    self.assertTrue(EventToDeliver.all().get() is None)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

  def testNoNewEntries(self):
    """Tests when there are no new entries."""
    FeedToFetch.insert([self.topic])
    self.entry_list = []
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)
    self.run_fetch_task()
    self.assertTrue(EventToDeliver.all().get() is None)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals(self.header_footer, record.header_footer)
    self.assertEquals(self.etag, record.etag)
    self.assertEquals(self.last_modified, record.last_modified)
    self.assertEquals('application/atom+xml', record.content_type)

    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

  def testPullError(self):
    """Tests when URLFetch raises an exception."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response, urlfetch_error=True)
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    task = testutil.get_tasks(main.FEED_RETRIES_QUEUE,
                              index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])
    self.assertEquals([(0, 1)], main.FETCH_SCORER.get_scores([self.topic]))

  def testPullRetry(self):
    """Tests that the task enqueued after a failure will run properly."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response, urlfetch_error=True)
    self.run_fetch_task()

    # Verify the failed feed was written to the Datastore.
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    testutil.get_tasks(main.FEED_RETRIES_QUEUE, expected_count=1)
    task = testutil.get_tasks(main.FEED_RETRIES_QUEUE,
                              index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])
    self.assertEquals([(0, 1)], main.FETCH_SCORER.get_scores([self.topic]))

    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response, urlfetch_error=True)
    self.handle('post', *task['params'].items())
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(2, feed.fetching_failures)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    testutil.get_tasks(main.FEED_RETRIES_QUEUE, expected_count=2)

  def testPullBadStatusCode(self):
    """Tests when the response status is bad."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 500, self.expected_response)
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)

    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    task = testutil.get_tasks(main.FEED_RETRIES_QUEUE,
                              index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])
    self.assertEquals([(0, 1)], main.FETCH_SCORER.get_scores([self.topic]))

  def testApiProxyError(self):
    """Tests when the APIProxy raises an error."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response, apiproxy_error=True)
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(self.topic))
    self.assertEquals(1, feed.fetching_failures)

    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    task = testutil.get_tasks(main.FEED_RETRIES_QUEUE,
                              index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])
    self.assertEquals([(0, 1)], main.FETCH_SCORER.get_scores([self.topic]))

  def testNoSubscribers(self):
    """Tests that when a feed has no subscribers we do not pull it."""
    self.assertTrue(Subscription.remove(self.callback, self.topic))
    db.put(KnownFeed.create(self.topic))
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is not None)
    self.entry_list = []
    FeedToFetch.insert([self.topic])
    self.run_fetch_task()

    # Verify that *no* feed entry records have been written.
    self.assertEquals([], FeedEntryRecord.get_entries_for_topic(
                               self.topic, self.all_ids))

    # And there is no EventToDeliver or tasks.
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=1)

    # And no scoring.
    self.assertEquals([(0, 0)], main.FETCH_SCORER.get_scores([self.topic]))

  def testRedirects(self):
    """Tests when redirects are encountered."""
    info = FeedRecord.get_or_create(self.topic)
    info.update(self.headers)
    info.put()
    FeedToFetch.insert([self.topic])

    real_topic = 'http://example.com/real-topic-location'
    self.headers['Location'] = real_topic
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 302, '',
        response_headers=self.headers.copy())

    del self.headers['Location']
    urlfetch_test_stub.instance.expect(
        'get', real_topic, 200, self.expected_response,
        response_headers=self.headers)

    self.run_fetch_task()
    self.assertTrue(EventToDeliver.all().get() is not None)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)

    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

  def testTooManyRedirects(self):
    """Tests when too many redirects are encountered."""
    info = FeedRecord.get_or_create(self.topic)
    info.update(self.headers)
    info.put()
    FeedToFetch.insert([self.topic])

    last_topic = self.topic
    real_topic = 'http://example.com/real-topic-location'
    for i in xrange(main.MAX_REDIRECTS):
      next_topic = real_topic + str(i)
      self.headers['Location'] = next_topic
      urlfetch_test_stub.instance.expect(
          'get', last_topic, 302, '',
          response_headers=self.headers.copy())
      last_topic = next_topic

    self.run_fetch_task()
    self.assertTrue(EventToDeliver.all().get() is None)

    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    task = testutil.get_tasks(main.FEED_RETRIES_QUEUE,
                              index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])

    self.assertEquals([(0, 1)], main.FETCH_SCORER.get_scores([self.topic]))

  def testRedirectToBadUrl(self):
    """Tests when the redirect URL is bad."""
    info = FeedRecord.get_or_create(self.topic)
    info.update(self.headers)
    info.put()
    FeedToFetch.insert([self.topic])

    real_topic = '/not/a/valid-redirect-location'
    self.headers['Location'] = real_topic
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 302, '',
        response_headers=self.headers.copy())

    self.run_fetch_task()
    self.assertTrue(EventToDeliver.all().get() is None)
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

    self.assertEquals([(0, 1)], main.FETCH_SCORER.get_scores([self.topic]))

  def testPutSplitting(self):
    """Tests that put() calls for feed records are split when too large."""
    # Make the content way too big.
    content_template = ('content' * 100 + '%s')
    self.all_ids = [str(i) for i in xrange(1000)]
    self.entry_payloads = [
      (content_template % entry_id) for entry_id in self.all_ids
    ]
    self.entry_list = [
        FeedEntryRecord.create_entry_for_topic(
            self.topic, entry_id, 'content%s' % entry_id)
        for entry_id in self.all_ids
    ]

    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)

    old_max_new = main.MAX_NEW_FEED_ENTRY_RECORDS
    main.MAX_NEW_FEED_ENTRY_RECORDS = len(self.all_ids) + 1
    try:
        self.run_fetch_task()
    finally:
      main.MAX_NEW_FEED_ENTRY_RECORDS = old_max_new

    # Verify that all feed entry records have been written along with the
    # EventToDeliver and FeedRecord.
    feed_entries = list(FeedEntryRecord.all())
    self.assertEquals(
        set(sha1_hash(k) for k in self.all_ids),
        set(e.id_hash for e in feed_entries))

    work = EventToDeliver.all().get()
    event_key = work.key()
    self.assertEquals(self.topic, work.topic)
    self.assertTrue('\n'.join(self.entry_payloads) in work.payload)
    work.delete()

    record = FeedRecord.get_or_create(self.topic)
    self.assertEquals(self.header_footer, record.header_footer)
    self.assertEquals(self.etag, record.etag)
    self.assertEquals(self.last_modified, record.last_modified)
    self.assertEquals('application/atom+xml', record.content_type)

    task = testutil.get_tasks(main.EVENT_QUEUE, index=0, expected_count=1)
    self.assertEquals(str(event_key), task['params']['event_key'])
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    testutil.get_tasks(main.FEED_RETRIES_QUEUE, expected_count=0)
    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

  def testPutSplittingFails(self):
    """Tests when splitting put() calls still doesn't help and we give up."""
    # Make the content way too big.
    content_template = ('content' * 150 + '%s')
    self.all_ids = [str(i) for i in xrange(1000)]
    self.entry_payloads = [
      (content_template % entry_id) for entry_id in self.all_ids
    ]
    self.entry_list = [
        FeedEntryRecord.create_entry_for_topic(
            self.topic, entry_id, 'content%s' % entry_id)
        for entry_id in self.all_ids
    ]

    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)

    old_splitting_attempts = main.PUT_SPLITTING_ATTEMPTS
    old_max_saves = main.MAX_FEED_RECORD_SAVES
    old_max_new = main.MAX_NEW_FEED_ENTRY_RECORDS
    main.PUT_SPLITTING_ATTEMPTS = 1
    main.MAX_FEED_RECORD_SAVES = len(self.entry_list) + 1
    main.MAX_NEW_FEED_ENTRY_RECORDS = main.MAX_FEED_RECORD_SAVES
    try:
      self.run_fetch_task()
    finally:
      main.PUT_SPLITTING_ATTEMPTS = old_splitting_attempts
      main.MAX_FEED_RECORD_SAVES = old_max_saves
      main.MAX_NEW_FEED_ENTRY_RECORDS = old_max_new

    # Verify that *NO* FeedEntryRecords or EventToDeliver has been written,
    # the FeedRecord wasn't updated, and no tasks were enqueued.
    self.assertEquals([], list(FeedEntryRecord.all()))
    self.assertEquals(None, EventToDeliver.all().get())

    record = FeedRecord.all().get()
    self.assertEquals(None, record)

    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

    # Put splitting failure does not count against the feed.
    self.assertEquals([(1, 0)], main.FETCH_SCORER.get_scores([self.topic]))

  def testFeedTooLarge(self):
    """Tests when the pulled feed's content size is too large."""
    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, '',
        response_headers=self.headers,
        urlfetch_size_error=True)
    self.run_fetch_task()
    self.assertEquals([], list(FeedEntryRecord.all()))
    self.assertEquals(None, EventToDeliver.all().get())
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

    self.assertEquals([(0, 1)], main.FETCH_SCORER.get_scores([self.topic]))

  def testTooManyNewEntries(self):
    """Tests when there are more new entries than we can handle at once."""
    self.all_ids = [str(i) for i in xrange(1000)]
    self.entry_payloads = [
      'content%s' % entry_id for entry_id in self.all_ids
    ]
    self.entry_list = [
        FeedEntryRecord.create_entry_for_topic(
            self.topic, entry_id, 'content%s' % entry_id)
        for entry_id in self.all_ids
    ]

    FeedToFetch.insert([self.topic])
    urlfetch_test_stub.instance.expect(
        'get', self.topic, 200, self.expected_response,
        response_headers=self.headers)

    self.run_fetch_task()

    # Verify that a subset of the entry records are present and the payload
    # only has the first N entries.
    feed_entries = FeedEntryRecord.get_entries_for_topic(
        self.topic, self.all_ids)
    expected_records = main.MAX_NEW_FEED_ENTRY_RECORDS
    self.assertEquals(
        [sha1_hash(k) for k in self.all_ids[:expected_records]],
        [e.id_hash for e in feed_entries])

    work = EventToDeliver.all().get()
    event_key = work.key()
    self.assertEquals(self.topic, work.topic)
    expected_content = '\n'.join(self.entry_payloads[:expected_records])
    self.assertTrue(expected_content in work.payload)
    self.assertFalse('content%d' % expected_records in work.payload)
    work.delete()

    record = FeedRecord.all().get()
    self.assertNotEquals(self.etag, record.etag)

    task = testutil.get_tasks(main.EVENT_QUEUE, index=0, expected_count=1)
    self.assertEquals(str(event_key), task['params']['event_key'])
    testutil.get_tasks(main.FEED_QUEUE, expected_count=1)
    task = testutil.get_tasks(main.FEED_RETRIES_QUEUE,
                              index=0, expected_count=1)
    self.assertEquals(self.topic, task['params']['topic'])
    self.assertEquals([(0, 1)], main.FETCH_SCORER.get_scores([self.topic]))

  def testNotAllowed(self):
    """Tests when the URL fetch is blocked due to URL scoring."""
    dos.DISABLE_FOR_TESTING = False
    try:
      main.FETCH_SCORER.blackhole([self.topic])
      start_scores = main.FETCH_SCORER.get_scores([self.topic])

      info = FeedRecord.get_or_create(self.topic)
      info.update(self.headers)
      info.put()
      FeedToFetch.insert([self.topic])
      self.run_fetch_task()

      # Verify that *no* feed entry records have been written.
      self.assertEquals([], FeedEntryRecord.get_entries_for_topic(
                                 self.topic, self.all_ids))

      # And there is no EventToDeliver or tasks.
      testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)
      tasks = testutil.get_tasks(main.FEED_QUEUE, expected_count=1)

      self.assertEquals(
          start_scores,
          main.FETCH_SCORER.get_scores([self.topic]))
    finally:
      dos.DISABLE_FOR_TESTING = True


class PullFeedHandlerTestWithParsing(testutil.HandlerTestBase):

  handler_class = main.PullFeedHandler

  def run_fetch_task(self, index=0):
    """Runs the currently enqueued fetch task."""
    task = testutil.get_tasks(main.FEED_QUEUE, index=index)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = task['name']
    try:
      self.handle('post')
    finally:
      del os.environ['HTTP_X_APPENGINE_TASKNAME']

  def testPullBadContent(self):
    """Tests when the content doesn't parse correctly."""
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect(
        'get', topic, 200, 'this does not parse')
    self.run_fetch_task()
    # No retry task should be written.
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)

  def testPullBadFeed(self):
    """Tests when the content parses, but is not a good Atom document."""
    data = ('<?xml version="1.0" encoding="utf-8"?>\n'
            '<meep><entry>wooh</entry></meep>')
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect('get', topic, 200, data)
    self.run_fetch_task()
    # No retry task should be written.
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)

  def testPullBadEncoding(self):
    """Tests when the content has a bad character encoding."""
    data = ('<?xml version="1.0" encoding="x-windows-874"?>\n'
            '<feed><my header="data"/>'
            '<entry><id>1</id><updated>123</updated>wooh</entry></feed>')
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect('get', topic, 200, data)
    self.run_fetch_task()
    # No retry task should be written.
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)

  def testPullGoodAtom(self):
    """Tests when the Atom XML can parse just fine."""
    data = ('<?xml version="1.0" encoding="utf-8"?>\n<feed><my header="data"/>'
            '<entry><id>1</id><updated>123</updated>wooh</entry></feed>')
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect('get', topic, 200, data)
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)
    event = EventToDeliver.all().get()
    self.assertEquals(data.replace('\n', ''), event.payload.replace('\n', ''))
    self.assertEquals('application/atom+xml', event.content_type)
    self.assertEquals('atom', FeedRecord.all().get().format)

  def testPullWithUnicodeEtag(self):
    """Tests when the ETag header has a unicode value.

    The ETag value should be ignored because non-ascii ETag values are invalid.
    """
    data = ('<?xml version="1.0" encoding="utf-8"?>\n<feed><my header="data"/>'
            '<entry><id>1</id><updated>123</updated>wooh</entry></feed>')
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect('get', topic, 200, data,
      response_headers={
        'ETag': '\xe3\x83\x96\xe3\x83\xad\xe3\x82\xb0\xe8\xa1\x86',
        'Content-Type': 'application/atom+xml',
    })
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)
    event = EventToDeliver.all().get()
    self.assertEquals(data.replace('\n', ''), event.payload.replace('\n', ''))
    self.assertEquals('application/atom+xml', event.content_type)
    self.assertEquals(
        {'Connection': 'cache-control',
         'Cache-Control': 'no-cache no-store max-age=1'},
        FeedRecord.all().get().get_request_headers())

  def testPullGoodRss(self):
    """Tests when the RSS XML can parse just fine."""
    data = ('<?xml version="1.0" encoding="utf-8"?>\n'
            '<rss version="2.0"><channel><my header="data"/>'
            '<item><guid>1</guid><updated>123</updated>wooh</item>'
            '</channel></rss>')
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect('get', topic, 200, data)
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)
    event = EventToDeliver.all().get()
    self.assertEquals(data.replace('\n', ''), event.payload.replace('\n', ''))
    self.assertEquals('application/rss+xml', event.content_type)
    self.assertEquals('rss', FeedRecord.all().get().format)

  def testPullGoodRdf(self):
    """Tests when the RDF (RSS 1.0) XML can parse just fine."""
    data = ('<?xml version="1.0" encoding="utf-8"?>\n'
            '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
            '<channel><my header="data"/></channel>'
            '<item><guid>1</guid><updated>123</updated>wooh</item>'
            '</rdf:RDF>')
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect('get', topic, 200, data)
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)
    event = EventToDeliver.all().get()
    self.assertEquals(data.replace('\n', ''), event.payload.replace('\n', ''))
    self.assertEquals('application/rdf+xml', event.content_type)
    self.assertEquals('rss', FeedRecord.all().get().format)

  def testPullArbitrary(self):
    """Tests pulling content of an arbitrary type."""
    data = 'this is my random payload of data'
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect(
        'get', topic, 200, data,
        response_headers={'Content-Type': 'my crazy content type'})
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)
    event = EventToDeliver.all().get()
    self.assertEquals(data, event.payload)
    self.assertEquals('my crazy content type', event.content_type)
    self.assertEquals('arbitrary', FeedRecord.all().get().format)

  def testPullBinaryContent(self):
    """Tests pulling binary content."""
    data = '\xff\x12 some binary data'
    topic = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))
    FeedToFetch.insert([topic])
    urlfetch_test_stub.instance.expect(
        'get', topic, 200, data,
        response_headers={'Content-Type': 'my crazy content type'})
    self.run_fetch_task()
    feed = FeedToFetch.get_by_key_name(get_hash_key_name(topic))
    self.assertTrue(feed is None)
    event = EventToDeliver.all().get()
    self.assertEquals(data, event.payload)
    self.assertEquals('my crazy content type', event.content_type)
    self.assertEquals('arbitrary', FeedRecord.all().get().format)

  def testMultipleFetch(self):
    """Tests doing multiple fetches asynchronously in parallel.

    Exercises the fork-join queue part of the fetching pipeline.
    """
    data = ('<?xml version="1.0" encoding="utf-8"?>\n<feed><my header="data"/>'
            '<entry><id>1</id><updated>123</updated>wooh</entry></feed>')
    topic_base = 'http://example.com/my-topic'
    callback = 'http://example.com/my-subscriber'
    topic_list = [topic_base + '1', topic_base + '2', topic_base + '3']
    FeedToFetch.insert(topic_list)
    for topic in topic_list:
      urlfetch_test_stub.instance.expect('get', topic, 200, data)
      self.assertTrue(Subscription.insert(callback, topic, 'token', 'secret'))

    os.environ['HTTP_X_APPENGINE_TASKNAME'] = testutil.get_tasks(
        main.FEED_QUEUE, index=0, expected_count=1)['name']
    try:
      self.handle('post')
    finally:
      del os.environ['HTTP_X_APPENGINE_TASKNAME']

    # Feed to fetch removed.
    self.assertEquals([], list(FeedToFetch.all()))
    self.assertEquals([(3, 0), (3, 0), (3, 0)],  # 3 because of shared domain
                      main.FETCH_SCORER.get_scores(topic_list))

    # All events written and correct.
    all_events = list(EventToDeliver.all())
    all_topics = [e.topic for e in all_events]
    self.assertEquals(3, len(all_events))
    self.assertEquals(set(topic_list), set(all_topics))
    event_tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=3)
    self.assertEquals(set(str(e.key()) for e in all_events),
                      set(task['params']['event_key'] for task in event_tasks))

    # All feed records written.
    all_records = list(FeedEntryRecord.all())
    all_parents = set(db.Key.from_path(FeedRecord.kind(),
                                       FeedRecord.create_key_name(topic))
                      for topic in topic_list)
    found_parents = set(r.parent().key() for r in all_records)
    self.assertEquals(3, len(found_parents))
    self.assertEquals(found_parents, all_parents)

################################################################################

class PushEventHandlerTest(testutil.HandlerTestBase):

  handler_class = main.PushEventHandler

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)

    self.chunk_size = main.EVENT_SUBSCRIBER_CHUNK_SIZE
    self.topic = 'http://example.com/hamster-topic'
    # Order of these URL fetches is determined by the ordering of the hashes
    # of the callback URLs, so we need random extra strings here to get
    # alphabetical hash order.
    self.callback1 = 'http://example1.com/hamster-callback1-12'
    self.callback2 = 'http://example2.com/hamster-callback2'
    self.callback3 = 'http://example3.com/hamster-callback3-123456'
    self.callback4 = 'http://example4.com/hamster-callback4-123'
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

    self.header_footer_rss = '<rss><channel></channel></rss>'
    self.test_payloads_rss = [
        '<item>article1</item>',
        '<item>article2</item>',
        '<item>article3</item>',
    ]
    self.expected_payload_rss = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<rss><channel>\n'
        '<item>article1</item>\n'
        '<item>article2</item>\n'
        '<item>article3</item>\n'
        '</channel></rss>'
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
    self.assertTrue(Subscription.insert(
        self.callback1, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, 'token', 'secret'))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 200, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 204, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 299, '', request_payload=self.expected_payload)
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
    event.put()
    self.handle('post', ('event_key', str(event.key())))
    self.assertEquals([], list(EventToDeliver.all()))
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

    self.assertEquals(
        [(1, 0), (1, 0), (1, 0)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3]))

  def testHmacData(self):
    """Tests that the content is properly signed with an HMAC."""
    self.assertTrue(Subscription.insert(
        self.callback1, self.topic, 'token', 'secret3'))
    # Secret is empty on purpose here, so the verify_token will be used instead.
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, 'my-token', ''))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, 'token', 'secret-stuff'))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '',
        request_payload=self.expected_payload,
        request_headers={
            'Content-Type': 'application/atom+xml',
            'X-Hub-Signature': 'sha1=3e9caf971b0833d15393022f5f01a47adf597af5'})
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 200, '',
        request_payload=self.expected_payload,
        request_headers={
            'Content-Type': 'application/atom+xml',
            'X-Hub-Signature': 'sha1=4847815aae8578eff55d351bc84a159b9bd8846e'})
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 204, '',
        request_payload=self.expected_payload,
        request_headers={
            'Content-Type': 'application/atom+xml',
            'X-Hub-Signature': 'sha1=8b0a9da7204afa8ae04fc9439755c556b1e38d99'})
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
    event.put()
    self.handle('post', ('event_key', str(event.key())))
    self.assertEquals([], list(EventToDeliver.all()))
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

  def testRssContentType(self):
    """Tests that the content type of an RSS feed is properly supplied."""
    self.assertTrue(Subscription.insert(
        self.callback1, self.topic, 'token', 'secret'))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '',
        request_payload=self.expected_payload_rss,
        request_headers={
            'Content-Type': 'application/rss+xml',
            'X-Hub-Signature': 'sha1=1607313b6195af74f29158421f0a31aa25d680da'})
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.RSS, 'application/rss+xml',
        self.header_footer_rss, self.test_payloads_rss)
    event.put()
    self.handle('post', ('event_key', str(event.key())))
    self.assertEquals([], list(EventToDeliver.all()))
    testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

  def testExtraSubscribers(self):
    """Tests when there are more subscribers to contact after delivery."""
    self.assertTrue(Subscription.insert(
        self.callback1, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, 'token', 'secret'))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 1
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
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

    self.assertEquals(
        [(1, 0), (1, 0), (0, 0)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3]))

    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 204, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()
    self.assertEquals([], list(EventToDeliver.all()))

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=2)
    self.assertEquals([event_key] * 2,
                      [t['params']['event_key'] for t in tasks])

    self.assertEquals(
        [(1, 0), (1, 0), (1, 0)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3]))

  def testBrokenCallbacks(self):
    """Tests that when callbacks return errors and are saved for later."""
    self.assertTrue(Subscription.insert(
        self.callback1, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, 'token', 'secret'))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 2
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
    event.put()
    event_key = str(event.key())

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback2, 404, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    self.assertEquals(
        [(0, 1), (0, 1), (0, 0)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3]))

    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 500, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    self.assertEquals(
        [(0, 1), (0, 1), (0, 1)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3]))

    work = EventToDeliver.all().get()
    sub_list = Subscription.get(work.failed_callbacks)
    callback_list = [sub.callback for sub in sub_list]
    self.assertEquals([self.callback1, self.callback2, self.callback3],
                      callback_list)

    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)
    tasks.extend(testutil.get_tasks(main.EVENT_RETRIES_QUEUE, expected_count=1))
    self.assertEquals([event_key] * 2,
                      [t['params']['event_key'] for t in tasks])

  def testDeadlineError(self):
    """Tests that callbacks in flight at deadline will be marked as failed."""
    try:
      def deadline():
        raise runtime.DeadlineExceededError()
      main.async_proxy.wait = deadline

      self.assertTrue(Subscription.insert(
          self.callback1, self.topic, 'token', 'secret'))
      self.assertTrue(Subscription.insert(
          self.callback2, self.topic, 'token', 'secret'))
      self.assertTrue(Subscription.insert(
          self.callback3, self.topic, 'token', 'secret'))
      main.EVENT_SUBSCRIBER_CHUNK_SIZE = 2
      event = EventToDeliver.create_event_for_topic(
          self.topic, main.ATOM, 'application/atom+xml',
          self.header_footer, self.test_payloads)
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

      # In this case no reporting should happen, since we do not have
      # any more time in the runtime to report stats.
      self.assertEquals(
          [(0, 0), (0, 0), (0, 0)],
          main.DELIVERY_SCORER.get_scores(
              [self.callback1, self.callback2, self.callback3]))
    finally:
      main.async_proxy = async_apiproxy.AsyncAPIProxy()

  def testRetryLogic(self):
    """Tests that failed urls will be retried after subsequent failures.

    This is an end-to-end test for push delivery failures and retries. We'll
    simulate multiple times through the failure list.
    """
    self.assertTrue(Subscription.insert(
        self.callback1, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback3, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback4, self.topic, 'token', 'secret'))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
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

    self.assertEquals(
        [(0, 1), (1, 0), (0, 1), (0, 0)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3, self.callback4]))

    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 500, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    self.assertEquals(
        [(0, 1), (1, 0), (0, 1), (0, 1)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3, self.callback4]))

    # Now the retries.
    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 404, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 500, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    self.assertEquals(
        [(0, 2), (1, 0), (0, 2), (0, 2)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3, self.callback4]))

    urlfetch_test_stub.instance.expect(
        'post', self.callback1, 204, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 302, '', request_payload=self.expected_payload)
    urlfetch_test_stub.instance.expect(
        'post', self.callback4, 200, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    self.assertEquals(
        [(1, 2), (1, 0), (0, 3), (1, 2)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3, self.callback4]))

    urlfetch_test_stub.instance.expect(
        'post', self.callback3, 204, '', request_payload=self.expected_payload)
    self.handle('post', ('event_key', event_key))
    urlfetch_test_stub.instance.verify_and_reset()

    self.assertEquals(
        [(1, 2), (1, 0), (1, 3), (1, 2)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2, self.callback3, self.callback4]))

    self.assertEquals([], list(EventToDeliver.all()))
    tasks = testutil.get_tasks(main.EVENT_QUEUE, expected_count=1)
    tasks.extend(testutil.get_tasks(main.EVENT_RETRIES_QUEUE, expected_count=3))
    self.assertEquals([event_key] * 4,
                      [t['params']['event_key'] for t in tasks])

  def testUrlFetchFailure(self):
    """Tests the UrlFetch API raising exceptions while sending notifications."""
    self.assertTrue(Subscription.insert(
        self.callback1, self.topic, 'token', 'secret'))
    self.assertTrue(Subscription.insert(
        self.callback2, self.topic, 'token', 'secret'))
    main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, self.test_payloads)
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
        main.EVENT_RETRIES_QUEUE, index=0, expected_count=1)
        ['params']['event_key'])

    self.assertEquals(
        [(0, 1), (0, 1)],
        main.DELIVERY_SCORER.get_scores(
            [self.callback1, self.callback2]))

  def testNotAllowed(self):
    """Tests pushing events to a URL that's not allowed due to scoring."""
    dos.DISABLE_FOR_TESTING = False
    try:
      main.DELIVERY_SCORER.blackhole([self.callback2])
      start_scores = main.DELIVERY_SCORER.get_scores([self.callback2])

      self.assertTrue(Subscription.insert(
          self.callback1, self.topic, 'token', 'secret'))
      self.assertTrue(Subscription.insert(
          self.callback2, self.topic, 'token', 'secret'))
      self.assertTrue(Subscription.insert(
          self.callback3, self.topic, 'token', 'secret'))
      main.EVENT_SUBSCRIBER_CHUNK_SIZE = 3
      urlfetch_test_stub.instance.expect(
          'post', self.callback1, 204, '',
          request_payload=self.expected_payload)
      urlfetch_test_stub.instance.expect(
          'post', self.callback3, 204, '',
          request_payload=self.expected_payload)
      event = EventToDeliver.create_event_for_topic(
          self.topic, main.ATOM, 'application/atom+xml',
          self.header_footer, self.test_payloads)
      event.put()
      self.handle('post', ('event_key', str(event.key())))
      self.assertEquals([], list(EventToDeliver.all()))
      testutil.get_tasks(main.EVENT_QUEUE, expected_count=0)

      self.assertEquals(
          [(1, 0)] + start_scores + [(1, 0)],
          main.DELIVERY_SCORER.get_scores(
              [self.callback1, self.callback2, self.callback3]))
    finally:
      dos.DISABLE_FOR_TESTING = True


class EventCleanupHandlerTest(testutil.HandlerTestBase):
  """Tests for the EventCleanupHandler worker."""

  def setUp(self):
    """Sets up the test harness."""
    self.now = datetime.datetime.utcnow()
    self.expire_time = self.now - datetime.timedelta(
        seconds=main.EVENT_CLEANUP_MAX_AGE_SECONDS)
    def create_handler():
      return main.EventCleanupHandler(now=lambda: self.now)
    self.handler_class = create_handler
    testutil.HandlerTestBase.setUp(self)
    self.topic = 'http://example.com/mytopic'
    self.header_footer = '<feed></feed>'

  def testEventCleanupTooYoung(self):
    """Tests when there are events present, but they're too young to remove."""
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, [])
    event.last_modified = self.expire_time + datetime.timedelta(seconds=1)
    event.put()
    self.handle('get')
    self.assertTrue(db.get(event.key()) is not None)

  def testEventCleanupOldEnough(self):
    """Tests when there are events old enough to clean up."""
    event = EventToDeliver.create_event_for_topic(
        self.topic, main.ATOM, 'application/atom+xml',
        self.header_footer, [])
    event.last_modified = self.expire_time
    event.put()

    too_young_event = EventToDeliver.create_event_for_topic(
        self.topic + 'blah', main.ATOM, 'application/atom+xml',
        self.header_footer, [])
    too_young_event.put()

    self.handle('get')
    self.assertTrue(db.get(event.key()) is None)
    self.assertTrue(db.get(too_young_event.key()) is not None)

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
        '&hub.lease_seconds=432000')

  def tearDown(self):
    """Tears down the test harness."""
    testutil.HandlerTestBase.tearDown(self)
    main.get_random_challenge = self.old_get_challenge

  def verify_record_task(self, topic):
    """Tests there is a valid KnownFeedIdentity task enqueued.
  
    Args:
      topic: The topic the task should be for.

    Raises:
      AssertionError if the task isn't there.
    """
    task = testutil.get_tasks(main.MAPPINGS_QUEUE, index=0, expected_count=1)
    self.assertEquals(topic, task['params']['topic'])

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
    self.verify_record_task(self.topic)

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
    self.verify_record_task(self.topic)

    # Async unsubscribe queues removal, but does not change former state.
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(202, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)

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

    # Async un-subscription does not change previous subscription state.
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'unsubscribe'),
        ('hub.verify', 'async'),
        ('hub.verify_token', self.verify_token))
    self.assertEquals(202, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)

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
    self.verify_record_task(self.topic)

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
        '&hub.lease_seconds=864000')
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
    self.verify_record_task(self.topic)

  def testDefaultLeaseSeconds(self):
    """Tests when the lease_seconds parameter is ommitted."""
    sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)

    self.verify_callback_querystring_template = (
        self.callback +
        '?hub.verify_token=the_token'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic'
        '&hub.mode=%s'
        '&hub.lease_seconds=432000')
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)
    self.handle('post',
        ('hub.callback', self.callback),
        ('hub.topic', self.topic),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', self.verify_token),
        ('hub.lease_seconds', ''))
    self.assertEquals(204, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.verify_record_task(self.topic)

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
    Subscription.insert(self.callback, self.topic, self.verify_token, 'secret')
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
    for exception in (runtime.DeadlineExceededError(), db.Error(),
                      apiproxy_errors.Error()):
      def new_confirm(*args):
        raise exception
      main.hooks.override_for_test(main.confirm_subscription, new_confirm)
      try:
        self.handle('post',
            ('hub.callback', self.callback),
            ('hub.topic', self.topic),
            ('hub.mode', 'subscribe'),
            ('hub.verify', 'sync'),
            ('hub.verify_token', self.verify_token))
        self.assertEquals(503, self.response_code())
      finally:
        main.hooks.reset_for_test(main.confirm_subscription)

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
        '&hub.lease_seconds=432000')
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
    self.verify_record_task(self.topic)

  def testSubscribeNormalization(self):
    """Tests that the topic and callback URLs are properly normalized."""
    self.topic += OTHER_STRING
    orig_callback = self.callback
    self.callback += OTHER_STRING
    sub_key = Subscription.create_key_name(
        main.normalize_iri(self.callback),
        main.normalize_iri(self.topic))
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    self.verify_callback_querystring_template = (
        orig_callback + '/~one:two/&='
        '?hub.verify_token=the_token'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic'
          '%%2F%%7Eone%%3Atwo%%2F%%26%%3D'
        '&hub.mode=%s'
        '&hub.lease_seconds=432000')
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
    self.verify_record_task(main.normalize_iri(self.topic))

  def testSubscribeIri(self):
    """Tests when the topic, callback, verify_token, and secrets are IRIs."""
    topic = self.topic + FUNNY_UNICODE
    topic_utf8 = self.topic + FUNNY_UTF8
    callback = self.callback + FUNNY_UNICODE
    callback_utf8 = self.callback + FUNNY_UTF8
    verify_token = self.verify_token + FUNNY_UNICODE
    verify_token_utf8 = self.verify_token + FUNNY_UTF8

    sub_key = Subscription.create_key_name(
        main.normalize_iri(callback),
        main.normalize_iri(topic))
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    self.verify_callback_querystring_template = (
        self.callback +
            '/blah/%%E3%%83%%96%%E3%%83%%AD%%E3%%82%%B0%%E8%%A1%%86'
        '?hub.verify_token=the_token%%2F'
            'blah%%2F%%E3%%83%%96%%E3%%83%%AD%%E3%%82%%B0%%E8%%A1%%86'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic%%2F'
            'blah%%2F%%25E3%%2583%%2596%%25E3%%2583%%25AD'
            '%%25E3%%2582%%25B0%%25E8%%25A1%%2586'
        '&hub.mode=%s'
        '&hub.lease_seconds=432000')
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)

    self.handle('post',
        ('hub.callback', callback_utf8),
        ('hub.topic', topic_utf8),
        ('hub.mode', 'subscribe'),
        ('hub.verify', 'sync'),
        ('hub.verify_token', verify_token_utf8))
    self.assertEquals(204, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.verify_record_task(self.topic + FUNNY_IRI)

  def testSubscribeUnicode(self):
    """Tests when UTF-8 encoded bytes show up in the requests.

    Technically this isn't well-formed or allowed by the HTTP/URI spec, but
    people do it anyways and we may as well allow it.
    """
    quoted_topic = urllib.quote(self.topic)
    topic = self.topic + FUNNY_UNICODE
    topic_utf8 = self.topic + FUNNY_UTF8
    quoted_callback = urllib.quote(self.callback)
    callback = self.callback + FUNNY_UNICODE
    callback_utf8 = self.callback + FUNNY_UTF8
    quoted_verify_token = urllib.quote(self.verify_token)
    verify_token = self.verify_token + FUNNY_UNICODE
    verify_token_utf8 = self.verify_token + FUNNY_UTF8

    sub_key = Subscription.create_key_name(
        main.normalize_iri(callback),
        main.normalize_iri(topic))
    self.assertTrue(Subscription.get_by_key_name(sub_key) is None)
    self.verify_callback_querystring_template = (
        self.callback +
            '/blah/%%E3%%83%%96%%E3%%83%%AD%%E3%%82%%B0%%E8%%A1%%86'
        '?hub.verify_token=the_token%%2F'
            'blah%%2F%%E3%%83%%96%%E3%%83%%AD%%E3%%82%%B0%%E8%%A1%%86'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic%%2F'
            'blah%%2F%%25E3%%2583%%2596%%25E3%%2583%%25AD'
            '%%25E3%%2582%%25B0%%25E8%%25A1%%2586'
        '&hub.mode=%s'
        '&hub.lease_seconds=432000')
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)

    payload = (
        'hub.callback=' + quoted_callback + FUNNY_UTF8 +
        '&hub.topic=' + quoted_topic + FUNNY_UTF8 +
        '&hub.mode=subscribe'
        '&hub.verify=sync'
        '&hub.verify_token=' + quoted_verify_token + FUNNY_UTF8)

    self.handle_body('post', payload)
    self.assertEquals(204, self.response_code())
    sub = Subscription.get_by_key_name(sub_key)
    self.assertTrue(sub is not None)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.verify_record_task(self.topic + FUNNY_IRI)


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
    self.secret = 'teh secrat'
    self.verify_callback_querystring_template = (
        self.callback +
        '?hub.verify_token=the_token'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic'
        '&hub.mode=%s'
        '&hub.lease_seconds=432000')

  def tearDown(self):
    """Verify that all URL fetches occurred."""
    testutil.HandlerTestBase.tearDown(self)
    main.get_random_challenge = self.old_get_challenge
    urlfetch_test_stub.instance.verify_and_reset()

  def verify_task(self, next_state):
    """Verifies that a subscription worker task is present.

    Args:
      next_state: The next state the task should cause the Subscription to have.
    """
    task = testutil.get_tasks(main.SUBSCRIPTION_QUEUE,
                              index=0, expected_count=1)
    params = task['params']
    self.assertEquals(self.sub_key, params['subscription_key_name'])
    self.assertEquals(next_state, params['next_state'])

  def verify_retry_task(self,
                        eta,
                        next_state,
                        verify_token=None,
                        secret=None,
                        auto_reconfirm=False):
    """Verifies that a subscription worker retry task is present.

    Args:
      eta: The ETA the retry task should have.
      next_state: The next state the task should cause the Subscription to have.
      verify_token: The verify token the retry task should have. Defaults to
        the current token.
      secret: The secret the retry task should have. Defaults to the
        current secret.
      auto_reconfirm: The confirmation type the retry task should have.
    """
    task = testutil.get_tasks(main.SUBSCRIPTION_QUEUE,
                              index=1, expected_count=2)
    params = task['params']
    self.assertEquals(testutil.task_eta(eta), task['eta'])
    self.assertEquals(self.sub_key, params['subscription_key_name'])
    self.assertEquals(next_state, params['next_state'])
    self.assertEquals(verify_token or self.verify_token, params['verify_token'])
    self.assertEquals(secret or self.secret, params['secret'])
    self.assertEquals(str(auto_reconfirm), params['auto_reconfirm'])

  def verify_no_record_task(self):
    """Tests there is not KnownFeedIdentity task enqueued.

    Raises:
      AssertionError if the task is there.
    """
    task = testutil.get_tasks(main.MAPPINGS_QUEUE, expected_count=0)

  def testNoWork(self):
    """Tests when a task is enqueued for a Subscription that doesn't exist."""
    self.handle('post', ('subscription_key_name', 'unknown'),
                        ('next_state', Subscription.STATE_VERIFIED))

  def testSubscribeSuccessful(self):
    """Tests when a subscription task is successful."""
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.request_insert(
        self.callback, self.topic, self.verify_token, self.secret)
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('secret', self.secret),
                        ('next_state', Subscription.STATE_VERIFIED))
    self.verify_task(Subscription.STATE_VERIFIED)
    self.verify_no_record_task()

    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertEquals(self.verify_token, sub.verify_token)
    self.assertEquals(self.secret, sub.secret)

  def testSubscribeSuccessfulQueryStringArgs(self):
    """Tests a subscription callback with querystring args."""
    self.callback += '?some=query&string=params&to=mess&it=up'
    self.sub_key = Subscription.create_key_name(self.callback, self.topic)
    self.assertTrue(db.get(KnownFeed.create_key(self.topic)) is None)
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.request_insert(
        self.callback, self.topic, self.verify_token, self.secret)
    self.verify_callback_querystring_template = (
        self.callback +
        '&hub.verify_token=the_token'
        '&hub.challenge=this_is_my_fake_challenge_string'
        '&hub.topic=http%%3A%%2F%%2Fexample.com%%2Fthe-topic'
        '&hub.mode=%s'
        '&hub.lease_seconds=432000')

    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'subscribe', 200,
        self.challenge)
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('secret', self.secret),
                        ('next_state', Subscription.STATE_VERIFIED))
    self.verify_task(Subscription.STATE_VERIFIED)
    self.verify_no_record_task()

    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertEquals(self.verify_token, sub.verify_token)
    self.assertEquals(self.secret, sub.secret)

  def testSubscribeFailed(self):
    """Tests when a subscription task fails."""
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.request_insert(
        self.callback, self.topic, self.verify_token, self.secret)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 500, '')
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('secret', self.secret),
                        ('next_state', Subscription.STATE_VERIFIED))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertEquals(1, sub.confirm_failures)
    self.assertEquals(self.verify_token, sub.verify_token)
    self.assertEquals(self.secret, sub.secret)
    self.verify_retry_task(sub.eta,
                           Subscription.STATE_VERIFIED,
                           verify_token=self.verify_token,
                           secret=self.secret)

  def testSubscribeConflict(self):
    """Tests when confirmation hits a conflict and archives the subscription."""
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.request_insert(
        self.callback, self.topic, self.verify_token, self.secret)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 404, '')
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('secret', self.secret),
                        ('next_state', Subscription.STATE_VERIFIED))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_TO_DELETE, sub.subscription_state)
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, expected_count=1)

  def testSubscribeBadChallengeResponse(self):
    """Tests when the subscriber responds with a bad challenge."""
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.request_insert(
        self.callback, self.topic, self.verify_token, self.secret)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 200, 'bad')
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('secret', self.secret),
                        ('next_state', Subscription.STATE_VERIFIED))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_NOT_VERIFIED, sub.subscription_state)
    self.assertEquals(1, sub.confirm_failures)
    self.verify_retry_task(sub.eta, Subscription.STATE_VERIFIED)

  def testUnsubscribeSuccessful(self):
    """Tests when an unsubscription request is successful."""
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.insert(
        self.callback, self.topic, self.verify_token, self.secret)
    Subscription.request_remove(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect(
        'get', self.verify_callback_querystring_template % 'unsubscribe', 200,
        self.challenge)
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('next_state', Subscription.STATE_TO_DELETE))
    self.verify_task(Subscription.STATE_TO_DELETE)
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)

  def testUnsubscribeFailed(self):
    """Tests when an unsubscription task fails."""
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.insert(
        self.callback, self.topic, self.verify_token, self.secret)
    Subscription.request_remove(self.callback, self.topic, self.verify_token)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'unsubscribe', 500, '')
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('next_state', Subscription.STATE_TO_DELETE),
                        ('secret', self.secret))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(1, sub.confirm_failures)
    self.verify_retry_task(sub.eta, Subscription.STATE_TO_DELETE)

  def testUnsubscribeGivesUp(self):
    """Tests when an unsubscription task completely gives up."""
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.insert(
        self.callback, self.topic, self.verify_token, self.secret)
    Subscription.request_remove(self.callback, self.topic, self.verify_token)
    sub = Subscription.get_by_key_name(self.sub_key)
    sub.confirm_failures = 100
    sub.put()
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'unsubscribe', 500, '')
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('next_state', Subscription.STATE_TO_DELETE))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(100, sub.confirm_failures)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.verify_task(Subscription.STATE_TO_DELETE)

  def testSubscribeOverwrite(self):
    """Tests that subscriptions can be overwritten with new parameters."""
    Subscription.insert(
        self.callback, self.topic, self.verify_token, self.secret)
    second_token = 'second_verify_token'
    second_secret = 'second secret'
    new_template = self.verify_callback_querystring_template.replace(
        self.verify_token, second_token)
    urlfetch_test_stub.instance.expect(
        'get', new_template % 'subscribe', 200, self.challenge)
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', second_token),
                        ('secret', second_secret),
                        ('next_state', Subscription.STATE_VERIFIED))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_VERIFIED, sub.subscription_state)
    self.assertEquals(second_token, sub.verify_token)
    self.assertEquals(second_secret, sub.secret)
    self.verify_no_record_task()

  def testConfirmError(self):
    """Tests when an exception is raised while confirming a subscription.

    This will just propagate up in the stack and cause the task to retry
    via the normal task queue retries.
    """
    called = [False]
    Subscription.request_insert(
        self.callback, self.topic, self.verify_token, self.secret)
    # All exceptions should just fall through.
    def new_confirm(*args, **kwargs):
      called[0] = True
      raise db.Error()
    try:
      main.hooks.override_for_test(main.confirm_subscription, new_confirm)
      try:
        self.handle('post', ('subscription_key_name', self.sub_key))
      except db.Error:
        pass
      else:
        self.fail()
    finally:
      main.hooks.reset_for_test(main.confirm_subscription)
    self.assertTrue(called[0])
    self.verify_task(Subscription.STATE_VERIFIED)

  def testRenewNack(self):
    """Tests when an auto-subscription-renewal returns a 404."""
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.insert(
        self.callback, self.topic, self.verify_token, self.secret)
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 404, '')
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('secret', self.secret),
                        ('next_state', Subscription.STATE_VERIFIED),
                        ('auto_reconfirm', 'True'))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_TO_DELETE, sub.subscription_state)
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, expected_count=0)

  def testRenewErrorFailure(self):
    """Tests when an auto-subscription-renewal returns errors repeatedly.

    In this case, since it's auto-renewal, the subscription should be dropped.
    """
    self.assertTrue(Subscription.get_by_key_name(self.sub_key) is None)
    Subscription.insert(
        self.callback, self.topic, self.verify_token, self.secret)
    sub = Subscription.get_by_key_name(self.sub_key)
    sub.confirm_failures = 100
    sub.put()
    urlfetch_test_stub.instance.expect('get',
        self.verify_callback_querystring_template % 'subscribe', 500, '')
    self.handle('post', ('subscription_key_name', self.sub_key),
                        ('verify_token', self.verify_token),
                        ('next_state', Subscription.STATE_VERIFIED),
                        ('auto_reconfirm', 'True'))
    sub = Subscription.get_by_key_name(self.sub_key)
    self.assertEquals(Subscription.STATE_TO_DELETE, sub.subscription_state)
    testutil.get_tasks(main.SUBSCRIPTION_QUEUE, expected_count=0)


class SubscriptionReconfirmHandlerTest(testutil.HandlerTestBase):
  """Tests for the periodic subscription reconfirming worker."""

  def testFullFlow(self):
    """Tests a full flow through the reconfirm worker."""
    self.now = time.time()
    self.called = False
    def start_map(*args, **kwargs):
      self.assertEquals(kwargs, {
          'name': 'Reconfirm expiring subscriptions',
          'reader_spec': 'offline_jobs.HashKeyDatastoreInputReader',
          'queue_name': 'polling',
          'handler_spec': 'offline_jobs.SubscriptionReconfirmMapper.run',
          'shard_count': 4,
          'reader_parameters': {
            'entity_kind': 'main.Subscription',
            'processing_rate': 100000,
            'threshold_timestamp':
                int(self.now + main.SUBSCRIPTION_CHECK_BUFFER_SECONDS),
          },
          'mapreduce_parameters': {
            'done_callback': '/work/cleanup_mapper',
            'done_callback_queue': 'polling',
          },
      })
      self.called = True

    def create_handler():
      return main.SubscriptionReconfirmHandler(
          now=lambda: self.now,
          start_map=start_map)
    self.handler_class = create_handler

    os.environ['HTTP_X_APPENGINE_QUEUENAME'] = main.POLLING_QUEUE
    try:
      self.handle('get')
      task = testutil.get_tasks(main.POLLING_QUEUE, index=0, expected_count=1)
      self.handle('post')
    finally:
      del os.environ['HTTP_X_APPENGINE_QUEUENAME']

    self.assertTrue(self.called)


class SubscriptionCleanupHandlerTest(testutil.HandlerTestBase):
  """Tests fo the SubscriptionCleanupHandler."""

  handler_class = main.SubscriptionCleanupHandler

  def testEmpty(self):
    """Tests cleaning up empty subscriptions."""
    self.handle('get')

  def testCleanup(self):
    """Tests cleaning up a few deleted subscriptions."""
    callback = 'http://example.com/callback/%d'
    topic = 'http://example.com/mytopic'
    self.assertTrue(Subscription.insert(callback % 1, topic, '', ''))
    self.assertTrue(Subscription.insert(callback % 2, topic, '', ''))
    self.assertTrue(Subscription.insert(callback % 3, topic, '', ''))
    self.assertEquals(3 * [Subscription.STATE_VERIFIED],
                      [s.subscription_state for s in Subscription.all()])

    Subscription.archive(callback % 1, topic)
    self.handle('get')
    self.assertEquals(2 * [Subscription.STATE_VERIFIED],
                      [s.subscription_state for s in Subscription.all()])


class CleanupMapperHandlerTest(testutil.HandlerTestBase):
  """Tests for the CleanupMapperHandler."""

  handler_class = main.CleanupMapperHandler

  def testMissing(self):
    """Tests cleaning up a mapreduce that's not present."""
    self.assertEquals([], list(mapreduce.model.MapreduceState.all()))
    os.environ['HTTP_MAPREDUCE_ID'] = '12345'
    try:
      self.handle('post')
    finally:
      del os.environ['HTTP_MAPREDUCE_ID']
    self.assertEquals([], list(mapreduce.model.MapreduceState.all()))

  def testPresent(self):
    """Tests cleaning up a mapreduce that's present."""
    mapreduce_id = mapreduce.control.start_map(
        name='Reconfirm expiring subscriptions',
        handler_spec='offline_jobs.SubscriptionReconfirmMapper.run',
        reader_spec='mapreduce.input_readers.DatastoreInputReader',
        reader_parameters=dict(
            processing_rate=100000,
            entity_kind='main.Subscription'))

    self.assertEquals(1, len(list(mapreduce.model.MapreduceState.all())))
    os.environ['HTTP_MAPREDUCE_ID'] = mapreduce_id
    try:
      self.handle('post')
    finally:
      del os.environ['HTTP_MAPREDUCE_ID']
    self.assertEquals([], list(mapreduce.model.MapreduceState.all()))

################################################################################

PollingMarker = main.PollingMarker


class TakePollingActionTest(unittest.TestCase):
  """Tests for the take_polling_action function."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()

  def testFailure(self):
    """Tests when inserting a new feed to fetch raises an exception."""
    called = [False]
    topics = ['one', 'two', 'three']
    @classmethod
    def new_insert(cls, topic_list, memory_only=True):
      called[0] = True
      self.assertFalse(memory_only)
      self.assertEquals(topic_list, topics)
      raise db.Error('Mock DB error')

    old_insert = main.FeedToFetch.insert
    main.FeedToFetch.insert = new_insert
    try:
      main.take_polling_action(['one', 'two', 'three'], '')
    finally:
      main.FeedToFetch.insert = old_insert

    self.assertTrue(called[0])


class PollBootstrapHandlerTest(testutil.HandlerTestBase):

  handler_class = main.PollBootstrapHandler

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.original_chunk_size = main.BOOSTRAP_FEED_CHUNK_SIZE
    main.BOOSTRAP_FEED_CHUNK_SIZE = 2
    os.environ['HTTP_X_APPENGINE_QUEUENAME'] = main.POLLING_QUEUE

  def tearDown(self):
    """Tears down the test harness."""
    testutil.HandlerTestBase.tearDown(self)
    main.BOOSTRAP_FEED_CHUNK_SIZE = self.original_chunk_size
    del os.environ['HTTP_X_APPENGINE_QUEUENAME']

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
    self.assertEquals('bootstrap', task['params']['poll_type'])

    # Now run the post handler with the params from this first task. It will
    # enqueue another task that starts *after* the last one in the chunk.
    self.handle('post', *task['params'].items())
    self.assertTrue(FeedToFetch.get_by_topic(topic) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is None)

    # Running this handler again will overwrite the FeedToFetch instances,
    # but it will not duplicate the polling queue Task in the chain of
    # iterating through all KnownFeed entries or the fork-join queue task that
    # will do the actual fetching.
    self.handle('post', *task['params'].items())
    task = testutil.get_tasks(main.POLLING_QUEUE, index=1, expected_count=2)
    self.assertEquals(sequence, task['params']['sequence'])
    self.assertEquals('bootstrap', task['params']['poll_type'])
    self.assertEquals(str(KnownFeed.create_key(topic2)),
                      task['params']['current_key'])
    self.assertTrue(task['name'].startswith(sequence))

    # Now running another post handler will handle the rest of the feeds.
    self.handle('post', *task['params'].items())
    self.assertTrue(FeedToFetch.get_by_topic(topic) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is not None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is not None)

    # Running this post handler again will do nothing because we de-dupe on
    # the continuation task to prevent doing any more work in the current cycle.
    self.handle('post', *task['params'].items())

    task_list = testutil.get_tasks(main.POLLING_QUEUE, expected_count=3)

    # Deal with a stupid race condition
    task = task_list[2]
    if 'params' not in task:
      task = task_list[3]

    self.assertEquals(sequence, task['params']['sequence'])
    self.assertEquals('bootstrap', task['params']['poll_type'])
    self.assertEquals(str(KnownFeed.create_key(topic3)),
                      task['params']['current_key'])
    self.assertTrue(task['name'].startswith(sequence))

    # Starting the cycle again will do nothing.
    self.handle('get')
    testutil.get_tasks(main.POLLING_QUEUE, expected_count=3)

    # Resetting the next start time to before the present time will
    # cause the iteration to start again.
    the_mark = PollingMarker.get()
    the_mark.next_start = \
        datetime.datetime.utcnow() - datetime.timedelta(seconds=120)
    db.put(the_mark)
    self.handle('get')

    task_list = testutil.get_tasks(main.POLLING_QUEUE, expected_count=4)
    task = task_list[3]
    self.assertNotEquals(sequence, task['params']['sequence'])

  def testRecord(self):
    """Tests when the parameter "poll_type=record" is specified."""
    topic = 'http://example.com/feed1'
    topic2 = 'http://example.com/feed2'
    topic3 = 'http://example.com/feed3-124'  # alphabetical on the hash of this
    db.put([KnownFeed.create(topic), KnownFeed.create(topic2),
            KnownFeed.create(topic3)])
    self.assertTrue(FeedToFetch.get_by_topic(topic) is None)
    self.assertTrue(FeedToFetch.get_by_topic(topic2) is None)
    self.assertTrue(FeedToFetch.get_by_topic(topic3) is None)

    # This will insert the initial task to start the polling process.
    self.handle('get', ('poll_type', 'record'))
    task = testutil.get_tasks(main.POLLING_QUEUE, index=0, expected_count=1)
    sequence = task['params']['sequence']
    self.assertEquals('record', task['params']['poll_type'])

    # Now run the post handler with the params from this first task. It will
    # enqueue another task that starts *after* the last one in the chunk.
    self.handle('post', *task['params'].items())
    task = testutil.get_tasks(main.POLLING_QUEUE, index=1, expected_count=2)
    self.assertEquals('record', task['params']['poll_type'])

    # Now running another post handler will handle the rest of the feeds.
    self.handle('post', *task['params'].items())

    # And there will be tasks in the MAPPINGS_QUEUE to update all of the
    # KnownFeeds that we have found.
    task = testutil.get_tasks(main.MAPPINGS_QUEUE, index=0, expected_count=3)
    self.assertEquals(topic, task['params']['topic'])
    task = testutil.get_tasks(main.MAPPINGS_QUEUE, index=1, expected_count=3)
    self.assertEquals(topic2, task['params']['topic'])
    task = testutil.get_tasks(main.MAPPINGS_QUEUE, index=2, expected_count=3)
    self.assertEquals(topic3, task['params']['topic'])

################################################################################

KnownFeedIdentity = main.KnownFeedIdentity


class RecordFeedHandlerTest(testutil.HandlerTestBase):
  """Tests for the RecordFeedHandler flow."""

  def setUp(self):
    """Sets up the test harness."""
    self.now = [datetime.datetime.utcnow()]
    self.handler_class = lambda: main.RecordFeedHandler(now=lambda: self.now[0])
    testutil.HandlerTestBase.setUp(self)

    self.old_identify = main.feed_identifier.identify
    self.expected_calls = []
    self.expected_results = []
    def new_identify(content, feed_type):
      self.assertEquals(self.expected_calls.pop(0), (content, feed_type))
      result = self.expected_results.pop(0)
      if isinstance(result, Exception):
        raise result
      else:
        return result

    main.feed_identifier.identify = new_identify
    self.topic = 'http://www.example.com/meepa'
    self.feed_id = 'my_feed_id'
    self.content = 'my_atom_content'

  def tearDown(self):
    """Tears down the test harness."""
    main.feed_identifier.identify = self.old_identify
    testutil.HandlerTestBase.tearDown(self)
    urlfetch_test_stub.instance.verify_and_reset()

  def verify_update(self):
    """Verifies the feed_id has been added for the topic."""
    feed_id = KnownFeedIdentity.get(KnownFeedIdentity.create_key(self.feed_id))
    feed = KnownFeed.get(KnownFeed.create_key(self.topic))
    self.assertEquals([self.topic], feed_id.topics)
    self.assertEquals(feed.feed_id, self.feed_id)
    self.assertEquals(feed.feed_id, feed_id.feed_id)

  def testNewFeed(self):
    """Tests recording details for a known feed."""
    urlfetch_test_stub.instance.expect('GET', self.topic, 200, self.content)
    self.expected_calls.append((self.content, 'atom'))
    self.expected_results.append(self.feed_id)
    self.handle('post', ('topic', self.topic))
    self.verify_update()

  def testNewFeedFetchFailure(self):
    """Tests when fetching a feed to record returns a non-200 response."""
    urlfetch_test_stub.instance.expect('GET', self.topic, 404, '')
    self.handle('post', ('topic', self.topic))
    feed = KnownFeed.get(KnownFeed.create_key(self.topic))
    self.assertTrue(feed.feed_id is None)

  def testNewFeedFetchException(self):
    """Tests when fetching a feed to record returns an exception."""
    urlfetch_test_stub.instance.expect('GET', self.topic, 200, '',
                                       urlfetch_error=True)
    self.handle('post', ('topic', self.topic))
    feed = KnownFeed.get(KnownFeed.create_key(self.topic))
    self.assertTrue(feed.feed_id is None)

  def testParseRetry(self):
    """Tests when parsing as Atom fails, but RSS is successful."""
    urlfetch_test_stub.instance.expect('GET', self.topic, 200, self.content)
    self.expected_calls.append((self.content, 'atom'))
    self.expected_results.append(xml.sax.SAXException('Mock error'))
    self.expected_calls.append((self.content, 'rss'))
    self.expected_results.append(self.feed_id)
    self.handle('post', ('topic', self.topic))
    self.verify_update()

  def testParseFails(self):
    """Tests when parsing completely fails."""
    urlfetch_test_stub.instance.expect('GET', self.topic, 200, self.content)
    self.expected_calls.append((self.content, 'atom'))
    self.expected_results.append(xml.sax.SAXException('Mock error'))
    self.expected_calls.append((self.content, 'rss'))
    self.expected_results.append(xml.sax.SAXException('Mock error 2'))
    self.handle('post', ('topic', self.topic))
    feed = KnownFeed.get(KnownFeed.create_key(self.topic))
    self.assertTrue(feed.feed_id is None)

  def testParseFindsNoIds(self):
    """Tests when no SAX exception is raised but no feed ID is found."""
    urlfetch_test_stub.instance.expect('GET', self.topic, 200, self.content)
    self.expected_calls.append((self.content, 'atom'))
    self.expected_results.append(None)
    self.expected_calls.append((self.content, 'rss'))
    self.expected_results.append(None)
    self.handle('post', ('topic', self.topic))
    feed = KnownFeed.get(KnownFeed.create_key(self.topic))
    self.assertTrue(feed.feed_id is None)

  def testParseFindsEmptyId(self):
    """Tests when no SAX exception is raised but the feed ID is empty."""
    urlfetch_test_stub.instance.expect('GET', self.topic, 200, self.content)
    self.expected_calls.append((self.content, 'atom'))
    self.expected_results.append('')
    self.handle('post', ('topic', self.topic))
    feed = KnownFeed.get(KnownFeed.create_key(self.topic))
    self.assertTrue(feed.feed_id is None)

  def testExistingFeedNeedsRefresh(self):
    """Tests recording details for an existing feed that needs a refresh."""
    KnownFeed.create(self.topic).put()
    self.now[0] += datetime.timedelta(
        seconds=main.FEED_IDENTITY_UPDATE_PERIOD + 1)

    urlfetch_test_stub.instance.expect('GET', self.topic, 200, self.content)
    self.expected_calls.append((self.content, 'atom'))
    self.expected_results.append(self.feed_id)
    self.handle('post', ('topic', self.topic))
    self.verify_update()

  def testExistingFeedNoRefresh(self):
    """Tests recording details when the feed does not need a refresh."""
    feed = KnownFeed.create(self.topic)
    feed.feed_id = 'meep'
    feed.put()
    self.handle('post', ('topic', self.topic))
    # Confirmed by no calls to urlfetch or feed_identifier.

  def testExistingFeedNoIdRefresh(self):
    """Tests that a KnownFeed with no ID will be refreshed."""
    feed = KnownFeed.create(self.topic)
    urlfetch_test_stub.instance.expect('GET', self.topic, 200, self.content)
    self.expected_calls.append((self.content, 'atom'))
    self.expected_results.append(self.feed_id)
    self.handle('post', ('topic', self.topic))
    self.verify_update()

  def testNewFeedRelation(self):
    """Tests when the feed ID relation changes for a topic."""
    KnownFeedIdentity.update(self.feed_id, self.topic)
    feed = KnownFeed.create(self.topic)
    feed.feed_id = self.feed_id
    feed.put()
    self.now[0] += datetime.timedelta(
        seconds=main.FEED_IDENTITY_UPDATE_PERIOD + 1)

    new_feed_id = 'other_feed_id'
    urlfetch_test_stub.instance.expect('GET', self.topic, 200, self.content)
    self.expected_calls.append((self.content, 'atom'))
    self.expected_results.append(new_feed_id)
    self.handle('post', ('topic', self.topic))

    feed_id = KnownFeedIdentity.get(KnownFeedIdentity.create_key(new_feed_id))
    feed = KnownFeed.get(feed.key())
    self.assertEquals([self.topic], feed_id.topics)
    self.assertEquals(feed.feed_id, new_feed_id)
    self.assertEquals(feed.feed_id, feed_id.feed_id)

    # Old KnownFeedIdentity should have been deleted.
    self.assertTrue(KnownFeedIdentity.get(
        KnownFeedIdentity.create_key(self.feed_id)) is None)


class RecordFeedHandlerWithParsingTest(testutil.HandlerTestBase):
  """Tests for the RecordFeedHandler that excercise parsing."""

  handler_class = main.RecordFeedHandler

  def testAtomParsing(self):
    """Tests parsing an Atom feed."""
    topic = 'http://example.com/atom'
    feed_id = 'my-id'
    data = ('<?xml version="1.0" encoding="utf-8"?>'
            '<feed><id>my-id</id></feed>')
    urlfetch_test_stub.instance.expect('GET', topic, 200, data)
    self.handle('post', ('topic', topic))

    known_id = KnownFeedIdentity.get(KnownFeedIdentity.create_key(feed_id))
    feed = KnownFeed.get(KnownFeed.create_key(topic))
    self.assertEquals([topic], known_id.topics)
    self.assertEquals(feed.feed_id, feed_id)
    self.assertEquals(feed.feed_id, known_id.feed_id)

  def testRssParsing(self):
    """Tests parsing an Atom feed."""
    topic = 'http://example.com/rss'
    feed_id = 'http://example.com/blah'
    data = ('<?xml version="1.0" encoding="utf-8"?><rss><channel>'
            '<link>http://example.com/blah</link></channel></rss>')
    urlfetch_test_stub.instance.expect('GET', topic, 200, data)
    self.handle('post', ('topic', topic))

    known_id = KnownFeedIdentity.get(KnownFeedIdentity.create_key(feed_id))
    feed = KnownFeed.get(KnownFeed.create_key(topic))
    self.assertEquals([topic], known_id.topics)
    self.assertEquals(feed.feed_id, feed_id)
    self.assertEquals(feed.feed_id, known_id.feed_id)

################################################################################

class HookManagerTest(unittest.TestCase):
  """Tests for the HookManager and Hook classes."""

  def setUp(self):
    """Sets up the test harness."""
    self.hooks_directory = tempfile.mkdtemp()
    if not os.path.exists(self.hooks_directory):
      os.makedirs(self.hooks_directory)
    self.valueA = object()
    self.valueB = object()
    self.valueC = object()
    self.funcA = lambda *a, **k: self.valueA
    self.funcB = lambda *a, **k: self.valueB
    self.funcC = lambda *a, **k: self.valueC
    self.globals_dict = {
      'funcA': self.funcA,
      'funcB': self.funcB,
      'funcC': self.funcC,
    }
    self.manager = main.HookManager()
    self.manager.declare(self.funcA)
    self.manager.declare(self.funcB)
    self.manager.declare(self.funcC)

  def tearDown(self):
    """Tears down the test harness."""
    shutil.rmtree(self.hooks_directory, True)

  def write_hook(self, filename, content):
    """Writes a test hook to the hooks directory.

    Args:
      filename: The relative filename the hook should have.
      content: The Python code that should go in the hook module.
    """
    hook_file = open(os.path.join(self.hooks_directory, filename), 'w')
    try:
      hook_file.write('#!/usr/bin/env python\n')
      hook_file.write(content)
    finally:
      hook_file.close()

  def load_hooks(self):
    """Causes the hooks to load."""
    self.manager.load(hooks_path=self.hooks_directory,
                      globals_dict=self.globals_dict)

  def testNoHooksDir(self):
    """Tests when there is no hooks directory present at all."""
    hooks_path = tempfile.mktemp()
    self.assertFalse(os.path.exists(hooks_path))
    self.manager.load(hooks_path=hooks_path,
                      globals_dict=self.globals_dict)
    for entry, hooks in self.manager._mapping.iteritems():
      self.assertEquals(0, len(hooks))

  def testNoHooks(self):
    """Tests loading a directory with no hooks modules."""
    self.load_hooks()
    self.assertEquals(self.valueA, self.manager.execute(self.funcA))
    self.assertEquals(self.valueB, self.manager.execute(self.funcB))
    self.assertEquals(self.valueC, self.manager.execute(self.funcC))

  def testOneGoodHook(self):
    """Tests a single good hook."""
    self.write_hook('my_hook.py',"""
class MyHook(Hook):
  def inspect(self, args, kwargs):
    return True
  def __call__(self, *args, **kwargs):
    return 'fancy string'
register(funcA, MyHook())
""")
    self.load_hooks()
    self.assertEquals('fancy string', self.manager.execute(self.funcA))

  def testDifferentHooksInOneModule(self):
    """Tests different hook methods in a single hook module."""
    self.write_hook('my_hook.py',"""
class MyHook(Hook):
  def __init__(self, value):
    self.value = value
  def inspect(self, args, kwargs):
    return True
  def __call__(self, *args, **kwargs):
    return self.value
register(funcA, MyHook('fancy A'))
register(funcB, MyHook('fancy B'))
register(funcC, MyHook('fancy C'))
""")
    self.load_hooks()
    self.assertEquals('fancy A', self.manager.execute(self.funcA))
    self.assertEquals('fancy B', self.manager.execute(self.funcB))
    self.assertEquals('fancy C', self.manager.execute(self.funcC))

  def testBadHookModule(self):
    """Tests a hook module that's bad and throws exception on load."""
    self.write_hook('my_hook.py',"""raise Exception('Doh')""")
    self.assertRaises(
        Exception,
        self.load_hooks)

  def testIncompleteHook(self):
    """Tests that an incomplete hook implementation will die on execute."""
    self.write_hook('my_hook1.py',"""
class MyHook(Hook):
  def inspect(self, args, kwargs):
    return True
register(funcA, MyHook())
""")
    self.load_hooks()
    self.assertRaises(
        AssertionError,
        self.manager.execute,
        self.funcA)

  def testHookModuleOrdering(self):
    """Tests that hook modules are loaded and applied in order."""
    self.write_hook('my_hook1.py',"""
class MyHook(Hook):
  def inspect(self, args, kwargs):
    args[0].append(1)
    return False
register(funcA, MyHook())
""")
    self.write_hook('my_hook2.py',"""
class MyHook(Hook):
  def inspect(self, args, kwargs):
    args[0].append(2)
    return False
register(funcA, MyHook())
""")
    self.write_hook('my_hook3.py',"""
class MyHook(Hook):
  def inspect(self, args, kwargs):
    return True
  def __call__(self, *args, **kwargs):
    return 'peanuts'
register(funcA, MyHook())
""")
    self.load_hooks()
    value_list = [5]
    self.assertEquals('peanuts', self.manager.execute(self.funcA, value_list))
    self.assertEquals([5, 1, 2], value_list)

  def testHookBadRegistration(self):
    """Tests when registering a hook for an unknown callable."""
    self.write_hook('my_hook1.py',"""
class MyHook(Hook):
  def inspect(self, args, kwargs):
    return False
register(lambda: None, MyHook())
""")
    self.assertRaises(
        main.InvalidHookError,
        self.load_hooks)

  def testMultipleRegistration(self):
    """Tests that the first hook is called when two are registered."""
    self.write_hook('my_hook.py',"""
class MyHook(Hook):
  def __init__(self, value):
    self.value = value
  def inspect(self, args, kwargs):
    args[0].append(self.value)
    return True
  def __call__(self, *args, **kwargs):
    return self.value
register(funcA, MyHook('fancy first'))
register(funcA, MyHook('fancy second'))
""")
    self.load_hooks()
    value_list = ['hello']
    self.assertEquals('fancy first',
                      self.manager.execute(self.funcA, value_list))
    self.assertEquals(['hello', 'fancy first', 'fancy second'], value_list)

################################################################################

if __name__ == '__main__':
  dos.DISABLE_FOR_TESTING = True
  unittest.main()
