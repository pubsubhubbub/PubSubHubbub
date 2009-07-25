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

"""Tests for the fat_publish module."""

import logging
logging.basicConfig(format='%(levelname)-8s %(filename)s] %(message)s')
import os
import sys
import unittest

# Run these tests from the 'hub' directory.
sys.path.insert(0, os.getcwd())
os.chdir('../hub')
sys.path.insert(0, os.getcwd())

import testutil
testutil.fix_path()

from google.appengine.ext import webapp

import main
import fat_publish

################################################################################

# Do aliasing that would happen anyways during Hook module loading.
fat_publish.sha1_hmac = main.sha1_hmac
fat_publish.FeedRecord = main.FeedRecord
fat_publish.Subscription = main.Subscription


class FatPingHandlerTest(testutil.HandlerTestBase):
  """Tests for the FatPingHandler class."""

  secret = 'thisismysecret'
  handler_class = fat_publish.create_handler(secret)

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.expected_headers = {
        'Content-Length': '-1',  # Used by WebOb for urlencoded POSTs.
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    self.topic = 'http://example.com/mytopic'
    self.fakefeed = 'my fake feed'
    self.fakefeed_signature = '5f9418a2e221ced6a0bc1263aaebcce297438740'
    self.success = False

    self.feed_record = main.FeedRecord.get_or_create(self.topic)
    main.Subscription.insert('callback', self.topic, 'token', 'secret')

    def parse_feed_mock(record, headers, body):
      self.assertEquals(self.feed_record.topic, record.topic)
      self.assertEquals(self.expected_headers, headers)
      self.assertEquals(self.fakefeed, body)
      return self.success

    fat_publish.parse_feed = parse_feed_mock

  def testNoSubscribers(self):
    """Tests when there are no subscribers."""
    self.success = False
    main.Subscription.remove('callback', self.topic)
    self.handle('post',
                ('topic', self.topic),
                ('content', self.fakefeed),
                ('signature', self.fakefeed_signature))
    self.assertEquals(204, self.response_code())

  def testSuccessfulParsing(self):
    """Tests when parsing is successful."""
    self.success = True
    self.handle('post',
                ('topic', self.topic),
                ('content', self.fakefeed),
                ('signature', self.fakefeed_signature))
    self.assertEquals(204, self.response_code())

  def testHeaders(self):
    """Tests that request headers are preserved."""
    HEADER = 'HTTP_MY_HEADER'
    self.expected_headers['My-Header'] = os.environ[HEADER] = 'cheese'
    try:
      self.success = True
      self.expected_headers
      self.handle('post',
                  ('topic', self.topic),
                  ('content', self.fakefeed),
                  ('signature', self.fakefeed_signature))
      self.assertEquals(204, self.response_code())
    finally:
      del os.environ[HEADER]

  def testParseFails(self):
    """Tests when parsing fails."""
    self.success = False
    self.handle('post',
                ('topic', self.topic),
                ('content', self.fakefeed),
                ('signature', self.fakefeed_signature))
    self.assertEquals(400, self.response_code())

  def testBadSignature(self):
    """Tests when the signature is present but invalid."""
    self.success = True
    self.handle('post',
                ('topic', self.topic),
                ('content', self.fakefeed),
                ('signature', 'bad'))
    self.assertEquals(403, self.response_code())

  def testMissingParams(self):
    """Tests when parameters are missing."""
    self.success = True
    # No signature.
    self.handle('post',
                ('topic', self.topic),
                ('content', self.fakefeed))
    self.assertEquals(400, self.response_code())

    # No topic.
    self.handle('post',
                ('content', self.fakefeed),
                ('signature', self.fakefeed_signature))
    self.assertEquals(400, self.response_code())

    # No content.
    self.handle('post',
                ('topic', self.topic),
                ('signature', self.fakefeed_signature))
    self.assertEquals(400, self.response_code())


class FatPublishHookTest(unittest.TestCase):
  """Tests for the FatPublishHook class."""

  def testCreateHook(self):
    """Tests creating a hook."""
    fat_handler = object()
    hook = fat_publish.FatPublishHook(fat_handler)
    handlers = [object(), object()]
    original_handlers = list(handlers)
    self.assertFalse(hook.inspect((handlers,), {}))
    self.assertEquals(original_handlers + [(r'/fatping', fat_handler)],
                      handlers)

################################################################################

if __name__ == '__main__':
  unittest.main()
