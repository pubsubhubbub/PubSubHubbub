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

__author__ = 'bslatkin@gmail.com (Brett Slatkin)'

"""Tests for the feed_identifier module."""

import logging
import os
import unittest
import xml.sax

import feed_identifier


class TestBase(unittest.TestCase):

  def setUp(self):
    self.testdata = os.path.join(os.path.dirname(__file__),
                                 'feed_diff_testdata')

  def load(self, path):
    return open(os.path.join(self.testdata, path)).read()


class AtomTest(TestBase):
  """Tests for identifying Atom-formatted feeds."""

  def testGood(self):
    feed_id = feed_identifier.identify(self.load('parsing.xml'), 'atom')
    self.assertEquals('tag:diveintomark.org,2001-07-29:/', feed_id)

  def testNoFeedId(self):
    feed_id = feed_identifier.identify(self.load('atom_no_id.xml'), 'atom')
    self.assertTrue(feed_id is None)

  def testIncorrectFormat(self):
    feed_id = feed_identifier.identify(self.load('rss_rdf.xml'), 'atom')
    self.assertTrue(feed_id is None)

  def testWhitespace(self):
    feed_id = feed_identifier.identify(self.load('whitespace_id.xml'), 'atom')
    self.assertEquals('my feed id here', feed_id)

  def testBadFormat(self):
    self.assertRaises(xml.sax.SAXParseException,
                      feed_identifier.identify,
                      self.load('bad_feed.xml'),
                      'atom')

  def testFullNamespace(self):
    feed_id = feed_identifier.identify(self.load('atom_namespace.xml'), 'atom')
    self.assertEquals('http://example.com/feeds/delta', feed_id)


class RssTest(TestBase):
  """Tests for identifying RSS-formatted feeds."""

  def testGood091(self):
    feed_id = feed_identifier.identify(self.load('sampleRss091.xml'), 'rss')
    self.assertEquals('http://writetheweb.com', feed_id)

  def testGood092(self):
    feed_id = feed_identifier.identify(self.load('sampleRss092.xml'), 'rss')
    self.assertEquals(
        'http://www.scripting.com/blog/categories/gratefulDead.html',
        feed_id)

  def testGood20(self):
    feed_id = feed_identifier.identify(self.load('rss2sample.xml'), 'rss')
    self.assertEquals('http://liftoff.msfc.nasa.gov/', feed_id)

  def testGoodRdf(self):
    feed_id = feed_identifier.identify(self.load('rss_rdf.xml'), 'rss')
    self.assertEquals('http://writetheweb.com', feed_id)

  def testNoFeedId(self):
    feed_id = feed_identifier.identify(self.load('rss_no_link.xml'), 'rss')
    self.assertTrue(feed_id is None)
  
  def testIncorrectFormat(self):
    feed_id = feed_identifier.identify(self.load('parsing.xml'), 'rss')
    self.assertTrue(feed_id is None)
  
  def testBadFormat(self):
    self.assertRaises(xml.sax.SAXParseException,
                      feed_identifier.identify,
                      self.load('bad_feed.xml'),
                      'rss')


if __name__ == '__main__':
  feed_identifier.DEBUG = True
  unittest.main()
