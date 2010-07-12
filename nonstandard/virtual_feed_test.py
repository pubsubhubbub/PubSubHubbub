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

"""Tests for the virtual_feed module."""

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
import virtual_feed

################################################################################

# Do aliasing that would happen anyways during Hook module loading.=
virtual_feed.sha1_hash = main.sha1_hash
virtual_feed.EventToDeliver = main.EventToDeliver


class InjectVirtualFeedTest(testutil.HandlerTestBase):
  """Tests for the inject_virtual_feed function."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.topic = 'http://example.com/my-topic/1'
    self.topic2 = 'http://example.com/my-topic/2'
    self.format = 'atom'
    self.header_footer = '<feed><id>tag:my-id</id>\n</feed>'
    self.entries_map = {
      'one': '<entry>first data</entry>',
      'two': '<entry>second data</entry>',
      'three': '<entry>third data</entry>',
    }
    os.environ['CURRENT_VERSION_ID'] = 'my-version.1234'
    virtual_feed.VIRTUAL_FEED_QUEUE.queue_name = 'default'

  def testInsertOneFragment(self):
    """Tests inserting one new fragment."""
    virtual_feed.inject_virtual_feed(
        self.topic, self.format, self.header_footer, self.entries_map)
    task = testutil.get_tasks('default', index=0, expected_count=1)
    self.assertTrue(task['name'].startswith(
        'fjq-FeedFragment-54124f41c1ea6e67e4beacac85b9f015e6830d41--'
        'my-version-'))
    results = virtual_feed.VIRTUAL_FEED_QUEUE.pop(task['name'])
    self.assertEquals(1, len(results))
    fragment = results[0]
    self.assertEquals(self.topic, fragment.topic)
    self.assertEquals(self.header_footer, fragment.header_footer)
    self.assertEquals(self.format, fragment.format)
    self.assertEquals(
        '<entry>third data</entry>\n'  # Hash order
        '<entry>second data</entry>\n'
        '<entry>first data</entry>',
        fragment.entries)

  def testInsertMultipleFragments(self):
    """Tests inserting multiple fragments on different virtual topics."""
    virtual_feed.inject_virtual_feed(
        self.topic, self.format, self.header_footer, self.entries_map)
    virtual_feed.inject_virtual_feed(
        self.topic2, self.format, self.header_footer, self.entries_map)

    task1, task2 = testutil.get_tasks('default', expected_count=2)
    self.assertTrue(task1['name'].startswith(
        'fjq-FeedFragment-54124f41c1ea6e67e4beacac85b9f015e6830d41--'
        'my-version-'))
    self.assertTrue(task2['name'].startswith(
        'fjq-FeedFragment-0449375bf584a7a5d3a09b344a726dead30c3927--'
        'my-version-'))

    virtual_feed.VIRTUAL_FEED_QUEUE.name = \
        'fjq-FeedFragment-54124f41c1ea6e67e4beacac85b9f015e6830d41-'
    fragment1 = virtual_feed.VIRTUAL_FEED_QUEUE.pop(task1['name'])[0]
    self.assertEquals(self.topic, fragment1.topic)

    virtual_feed.VIRTUAL_FEED_QUEUE.name = \
        'fjq-FeedFragment-0449375bf584a7a5d3a09b344a726dead30c3927-'
    fragment2 = virtual_feed.VIRTUAL_FEED_QUEUE.pop(task2['name'])[0]
    self.assertEquals(self.topic2, fragment2.topic)


class CollateFeedHandlerTest(testutil.HandlerTestBase):
  """Tests for the CollateFeedHandler class."""

  handler_class = virtual_feed.CollateFeedHandler

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.topic = 'http://example.com/my-topic/1'
    self.topic2 = 'http://example.com/my-topic/2'
    self.format = 'atom'
    self.header_footer = '<feed><id>tag:my-id</id>\n</feed>'
    self.entries_map = {
      'one': '<entry>first data</entry>',
      'two': '<entry>second data</entry>',
      'three': '<entry>third data</entry>',
    }
    os.environ['CURRENT_VERSION_ID'] = 'my-version.1234'
    virtual_feed.VIRTUAL_FEED_QUEUE.queue_name = 'default'

  def testNoWork(self):
    """Tests when the queue is empty."""
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = (
      'fjq-FeedFragment-54124f41c1ea6e67e4beacac85b9f01abb6830d41--'
      'my-version-42630240-2654435761-0')
    self.handle('post')

  def testOneFragment(self):
    """Tests when there is one fragment in the queue."""
    virtual_feed.inject_virtual_feed(
        self.topic, self.format, self.header_footer, self.entries_map)
    task = testutil.get_tasks('default', index=0, expected_count=1)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = task['name']
    self.handle('post')

    event_list = list(main.EventToDeliver.all())
    self.assertEquals(1, len(event_list))
    event = event_list[0]

    # No parent to ensure it's not rate limited by an entity group.
    self.assertEquals(None, event.key().parent())

    self.assertEquals(self.topic, event.topic)
    self.assertEquals(
      '<?xml version="1.0" encoding="utf-8"?>\n'
      '<feed><id>tag:my-id</id>\n\n'
      '<entry>third data</entry>\n'
      '<entry>second data</entry>\n'
      '<entry>first data</entry>\n'
      '</feed>',
      event.payload)
    self.assertEquals('application/atom+xml', event.content_type)
    self.assertEquals(1, event.max_failures)

    task = testutil.get_tasks('event-delivery', index=0, expected_count=1)
    self.assertEquals(str(event.key()), task['params']['event_key'])

  def testMultipleFragments(self):
    """Tests when there is more than one fragment in the queue."""
    virtual_feed.inject_virtual_feed(
        self.topic, self.format, self.header_footer, self.entries_map)
    virtual_feed.inject_virtual_feed(
        self.topic, self.format, self.header_footer, self.entries_map)
    task = testutil.get_tasks('default', index=0, expected_count=1)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = task['name']
    self.handle('post')

    event_list = list(main.EventToDeliver.all())
    self.assertEquals(1, len(event_list))
    event = event_list[0]

    self.assertEquals(self.topic, event.topic)
    self.assertEquals(
      '<?xml version="1.0" encoding="utf-8"?>\n'
      '<feed><id>tag:my-id</id>\n\n'
      '<entry>third data</entry>\n'
      '<entry>second data</entry>\n'
      '<entry>first data</entry>\n'
      '<entry>third data</entry>\n'
      '<entry>second data</entry>\n'
      '<entry>first data</entry>\n'
      '</feed>',
      event.payload)

  def testMultipleQueues(self):
    """Tests multiple virtual feeds and queues."""
    virtual_feed.inject_virtual_feed(
        self.topic, self.format, self.header_footer, self.entries_map)
    virtual_feed.inject_virtual_feed(
        self.topic2, self.format, self.header_footer, self.entries_map)
    task1, task2 = testutil.get_tasks('default', expected_count=2)

    os.environ['HTTP_X_APPENGINE_TASKNAME'] = task1['name']
    self.handle('post')

    os.environ['HTTP_X_APPENGINE_TASKNAME'] = task2['name']
    self.handle('post')

    event_list = list(main.EventToDeliver.all())
    self.assertEquals(2, len(event_list))
    self.assertEquals(self.topic, event_list[0].topic)
    self.assertEquals(self.topic2, event_list[1].topic)

################################################################################

if __name__ == '__main__':
  unittest.main()
