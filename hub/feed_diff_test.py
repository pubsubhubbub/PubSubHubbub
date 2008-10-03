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

__author__ = 'bslatkin@gmail.com (Brett Slatkin)'

"""Tests for the feed_diff module."""

import logging
import os
import unittest

import feed_diff


class FeedDiffTest(unittest.TestCase):
  
  def setUp(self):
    """Sets up the test harness."""
    self.testdata = os.path.join(os.path.dirname(__file__),
                                 'feed_diff_testdata')
  
  def testUptimeTimeFiltering(self):
    """Tests filtering by update time."""
    data = open(os.path.join(self.testdata, 'update_filtering.xml')).read()
    header_footer, entries = feed_diff.filter("2008-07-12T04:28:45Z", data)
    
    expected_list = [
      (u'tag:diveintomark.org,2008-07-12:/archives/20080712042845',
       u'2008-07-12T04:28:45Z'),
      (u'tag:diveintomark.org,2008-07-13:/archives/20080713011654',
       u'2008-07-13T04:19:40Z'),
      (u'tag:diveintomark.org,2008-07-17:/archives/20080717044506',
       u'2008-07-17T04:47:24Z'),
      (u'tag:diveintomark.org,2008-07-23:/archives/20080723030709',
       u'2008-07-23T16:55:41Z'),
      (u'tag:diveintomark.org,2008-07-29:/archives/20080729021401',
       u'2008-08-01T00:05:25Z'),
      (u'tag:diveintomark.org,2008-08-05:/archives/20080805020410',
       u'2008-08-05T02:04:10Z'),
      (u'tag:diveintomark.org,2008-08-05:/archives/20080805155619',
       u'2008-08-05T19:09:22Z'),
      (u'tag:diveintomark.org,2008-08-06:/archives/20080806144009',
       u'2008-08-06T14:40:09Z'),
      (u'tag:diveintomark.org,2008-08-07:/archives/20080807025755',
       u'2008-08-07T02:57:55Z'),
      (u'tag:diveintomark.org,2008-08-07:/archives/20080807233337',
       u'2008-08-12T01:23:03Z'),
      (u'tag:diveintomark.org,2008-08-12:/archives/20080812160843',
       u'2008-08-12T16:08:43Z'),
      (u'tag:diveintomark.org,2008-08-14:/archives/20080814215936',
       u'2008-08-14T23:08:54Z'),
    ]
    
    found_entries = sorted(entries.items())
    self.assertEqual(len(expected_list), len(found_entries))
    for expected, found in zip(expected_list, found_entries):
      expected_key, expected_updated = expected
      found_key, (found_updated, found_content) = found
      self.assertEqual(expected_key, found_key)
      self.assertEqual(expected_updated, found_updated)
      self.assertTrue(found_content)

  # TODO: Add more tests, of course...


if __name__ == '__main__':
  ## feed_diff.DEBUG = True
  ## logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
