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

"""Tests for the offline_jobs module."""

import datetime
import logging
logging.basicConfig(format='%(levelname)-8s %(filename)s] %(message)s')
import re
import time
import unittest

import testutil
testutil.fix_path()

from google.appengine.ext import db

from mapreduce import context
from mapreduce.lib import key_range

import main
import offline_jobs

################################################################################

class HashKeyDatastoreInputReaderTest(unittest.TestCase):
  """Tests for the HashKeyDatastoreInputReader."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.app = 'my-app-id'
    self.entity_kind = 'main.Subscription'
    self.namespace = 'my-namespace'

  def testOneShard(self):
    """Tests just one shard."""
    result = (
        offline_jobs.HashKeyDatastoreInputReader._split_input_from_namespace(
          self.app, self.namespace, self.entity_kind, 1))

    expected = [
      key_range.KeyRange(
          key_start=db.Key.from_path(
              'Subscription',
              u'hash_0000000000000000000000000000000000000000',
              _app=u'my-app-id'),
          key_end=db.Key.from_path(
              'Subscription',
              u'hash_ffffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          direction='ASC',
          include_start=True,
          include_end=True,
          namespace='my-namespace',
          _app='my-app-id')
    ]
    self.assertEquals(expected, result)

  def testTwoShards(self):
    """Tests two shares: one for number prefixes, one for letter prefixes."""
    result = (
        offline_jobs.HashKeyDatastoreInputReader._split_input_from_namespace(
          self.app, self.namespace, self.entity_kind, 2))

    expected = [
      key_range.KeyRange(
          key_start=db.Key.from_path(
              'Subscription',
              u'hash_0000000000000000000000000000000000000000',
              _app=u'my-app-id'),
          key_end=db.Key.from_path(
              'Subscription',
              u'hash_7fffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          direction='DESC',
          include_start=True,
          include_end=True,
          namespace='my-namespace',
          _app='my-app-id'),
      key_range.KeyRange(
          key_start=db.Key.from_path(
              'Subscription',
              u'hash_7fffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          key_end=db.Key.from_path(
              'Subscription',
              u'hash_ffffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          direction='ASC',
          include_start=False,
          include_end=True,
          namespace='my-namespace',
          _app='my-app-id'),
    ]
    self.assertEquals(expected, result)

  def testManyShards(self):
    """Tests having many shards with multiple levels of splits."""
    result = (
        offline_jobs.HashKeyDatastoreInputReader._split_input_from_namespace(
          self.app, self.namespace, self.entity_kind, 4))

    expected = [
      key_range.KeyRange(
          key_start=db.Key.from_path(
              'Subscription',
              u'hash_0000000000000000000000000000000000000000',
              _app=u'my-app-id'),
          key_end=db.Key.from_path(
              'Subscription',
              u'hash_3fffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          direction='DESC',
          include_start=True,
          include_end=True,
          namespace='my-namespace',
          _app='my-app-id'),
      key_range.KeyRange(
          key_start=db.Key.from_path(
              'Subscription',
              u'hash_3fffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          key_end=db.Key.from_path(
              'Subscription',
              u'hash_7fffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          direction='ASC',
          include_start=False,
          include_end=True,
          namespace='my-namespace',
          _app='my-app-id'),
      key_range.KeyRange(
          key_start=db.Key.from_path(
              'Subscription',
              u'hash_7fffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          key_end=db.Key.from_path(
              'Subscription',
              u'hash_bfffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          direction='DESC',
          include_start=False,
          include_end=True,
          namespace='my-namespace',
          _app='my-app-id'),
      key_range.KeyRange(
          key_start=db.Key.from_path(
              'Subscription',
              u'hash_bfffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          key_end=db.Key.from_path(
              'Subscription',
              u'hash_ffffffffffffffffffffffffffffffffffffffff',
              _app=u'my-app-id'),
          direction='ASC',
          include_start=False,
          include_end=True,
          namespace='my-namespace',
          _app='my-app-id'),
    ]
    self.assertEquals(expected, result)


Subscription = main.Subscription


class SubscriptionReconfirmMapperTest(unittest.TestCase):
  """Tests for the SubscriptionReconfirmMapper."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.mapper = offline_jobs.SubscriptionReconfirmMapper()
    self.callback = 'http://example.com/my-callback-url'
    self.topic = 'http://example.com/my-topic-url'
    self.token = 'token'
    self.secret = 'my secrat'

    self.now = datetime.datetime.utcnow()
    self.threshold_seconds = 1000
    self.threshold_timestamp = (
        time.mktime(self.now.utctimetuple()) + self.threshold_seconds)
    self.getnow = lambda: self.now

    class FakeMapper(object):
      params = {'threshold_timestamp': str(self.threshold_timestamp)}
    class FakeSpec(object):
      mapreduce_id = '1234'
      mapper = FakeMapper()
    self.context = context.Context(FakeSpec(), None)
    context.Context._set(self.context)

  def get_subscription(self):
    """Returns the Subscription used for testing."""
    return Subscription.get_by_key_name(
        Subscription.create_key_name(self.callback, self.topic))

  def testValidateParams(self):
    """Tests the validate_params static method."""
    self.assertRaises(
        AssertionError,
        offline_jobs.SubscriptionReconfirmMapper.validate_params,
        {})
    offline_jobs.SubscriptionReconfirmMapper.validate_params(
        {'threshold_timestamp': 123})

  def testIgnoreUnverified(self):
    """Tests that unverified subscriptions are skipped."""
    self.assertTrue(Subscription.request_insert(
        self.callback, self.topic, self.token, self.secret,
        now=self.getnow))
    sub = self.get_subscription()
    self.mapper.run(sub)
    testutil.get_tasks(main.POLLING_QUEUE, expected_count=0)

  def testAfterThreshold(self):
    """Tests when a subscription is not yet ready for reconfirmation."""
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret,
        now=self.getnow, lease_seconds=self.threshold_seconds))
    sub = self.get_subscription()
    self.mapper.run(sub)
    testutil.get_tasks(main.POLLING_QUEUE, expected_count=0)

  def testBeforeThreshold(self):
    """Tests when a subscription is ready for reconfirmation."""
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret,
        now=self.getnow, lease_seconds=self.threshold_seconds-1))
    sub = self.get_subscription()
    self.mapper.run(sub)
    task = testutil.get_tasks(main.POLLING_QUEUE, index=0, expected_count=1)
    self.assertEquals('polling', task['headers']['X-AppEngine-QueueName'])

################################################################################

class CountSubscribersTest(unittest.TestCase):
  """Tests for the CountSubscribers job."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.mapper = offline_jobs.CountSubscribers()
    self.callback = 'http://foo.callback-example.com/my-callback-url'
    self.topic = 'http://example.com/my-topic-url'
    self.token = 'token'
    self.secret = 'my secrat'
    # Do not make these raw strings on purpose, since they will get
    # passed through escaped in the mapreduce.yaml.
    self.topic_pattern = '^http://example\\.com/.*$'
    self.callback_pattern = (
        'http(?:s)?://(?:[^\\.]+\\.)*([^\\./]+\.[^\\./]+)(?:/.*)?')

    class FakeMapper(object):
      params = {
        'topic_pattern': self.topic_pattern,
        'callback_pattern': self.callback_pattern,
      }
    class FakeSpec(object):
      mapreduce_id = '1234'
      mapper = FakeMapper()
    self.context = context.Context(FakeSpec(), None)
    context.Context._set(self.context)

  def get_subscription(self):
    """Returns the Subscription used for testing."""
    self.assertTrue(Subscription.insert(
        self.callback, self.topic, self.token, self.secret))
    return Subscription.get_by_key_name(
        Subscription.create_key_name(self.callback, self.topic))

  def testExpressions(self):
    """Tests the default expressions we're going to use for callbacks."""
    callback_re = re.compile(self.callback_pattern)
    self.assertEquals(
        'blah.com',
        callback_re.match('http://foo.blah.com/stuff').group(1))
    self.assertEquals(
        'blah.com',
        callback_re.match('http://blah.com/stuff').group(1))
    self.assertEquals(
        'blah.com',
        callback_re.match('http://one.two.three.blah.com/stuff').group(1))
    self.assertEquals(
        'blah.com',
        callback_re.match('http://no-ending.blah.com').group(1))
    self.assertEquals(
        'example.com',
        callback_re.match('https://fun.with.https.example.com/').group(1))

  def testValidateParams(self):
    """Tests the validate_params function."""
    self.assertRaises(
        KeyError,
        offline_jobs.CountSubscribers.validate_params,
        {})
    self.assertRaises(
        AssertionError,
        offline_jobs.CountSubscribers.validate_params,
        {'topic_pattern': ''})
    self.assertRaises(
        re.error,
        offline_jobs.CountSubscribers.validate_params,
        {'topic_pattern': 'this is bad('})
    self.assertRaises(
        KeyError,
        offline_jobs.CountSubscribers.validate_params,
        {'topic_pattern': 'okay'})
    self.assertRaises(
        AssertionError,
        offline_jobs.CountSubscribers.validate_params,
        {'topic_pattern': 'okay', 'callback_pattern': ''})
    self.assertRaises(
        re.error,
        offline_jobs.CountSubscribers.validate_params,
        {'topic_pattern': 'okay', 'callback_pattern': 'this is bad('})
    offline_jobs.CountSubscribers.validate_params(
        {'topic_pattern': 'okay', 'callback_pattern': 'and okay'})

  def testTopicMatch_CallbackMatch(self):
    """Tests when the topic and callbacks match."""
    sub = self.get_subscription()
    gen = self.mapper.run(sub)
    counter = gen.next()
    self.assertEquals('callback-example.com', counter.counter_name)
    self.assertEquals(1, counter.delta)
    self.assertRaises(StopIteration, gen.next)

  def testTopicMatch_CallbackNoMatch(self):
    """Tests when the topic matches but the callback does not."""
    self.callback = 'some garbage'
    sub = self.get_subscription()
    gen = self.mapper.run(sub)
    self.assertRaises(StopIteration, gen.next)

  def testTopicNoMatch(self):
    """Tests when the topic does not match."""
    self.topic = 'http://does-not-match.com'
    sub = self.get_subscription()
    gen = self.mapper.run(sub)
    self.assertRaises(StopIteration, gen.next)

################################################################################

if __name__ == '__main__':
  unittest.main()
