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

"""Tests for the pubsubhubbub_publish module."""

__author__ = 'bslatkin@gmail.com (Brett Slatkin)'

import BaseHTTPServer
import urllib
import unittest
import threading

import pubsubhubbub_publish


REQUESTS = 0


class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  def do_POST(self):
    global REQUESTS
    print 'Accessed', self.path
    REQUESTS += 1

    length = int(self.headers.get('content-length', 0))
    if not length:
      return self.send_error(500)
    body = self.rfile.read(length)

    if self.path == '/single':
      if body != urllib.urlencode(
          {'hub.url': 'http://example.com/feed', 'hub.mode': 'publish'}):
        self.send_error(500)
        self.wfile.write('Bad body. Found:')
        self.wfile.write(body)
      else:
        self.send_response(204)
    elif self.path == '/multiple':
      if body != urllib.urlencode(
          {'hub.url': ['http://example.com/feed',
                       'http://example.com/feed2',
                       'http://example.com/feed3'],
           'hub.mode': 'publish'}, doseq=True):
        self.send_error(500)
        self.wfile.write('Bad body. Found:')
        self.wfile.write(body)
      else:
        self.send_response(204)
    elif self.path == '/batch':
      self.send_response(204)
    elif self.path == '/fail':
      self.send_error(400)
      self.wfile.write('bad argument')
    else:
      self.send_error(404)


class PublishTest(unittest.TestCase):

  def setUp(self):
    global REQUESTS
    REQUESTS = 0
    self.server = BaseHTTPServer.HTTPServer(('', 0), RequestHandler)
    t = threading.Thread(target=self.server.serve_forever)
    t.setDaemon(True)
    t.start()
    self.hub = 'http://%s:%d' % (
        self.server.server_name, self.server.server_port)
    self.feed = 'http://example.com/feed'
    self.feed2 = 'http://example.com/feed2'
    self.feed3 = 'http://example.com/feed3'

  def testSingle(self):
    pubsubhubbub_publish.publish(self.hub + '/single', self.feed)
    self.assertEquals(1, REQUESTS)

  def testMultiple(self):
    pubsubhubbub_publish.publish(self.hub + '/multiple',
                                 self.feed, self.feed2, self.feed3)

  def testList(self):
    pubsubhubbub_publish.publish(self.hub + '/multiple',
                                 [self.feed, self.feed2, self.feed3])

  def testIterable(self):
    pubsubhubbub_publish.publish(self.hub + '/multiple',
                                 iter([self.feed, self.feed2, self.feed3]))

  def testBatchSizeLimit(self):
    old = pubsubhubbub_publish.URL_BATCH_SIZE
    try:
      pubsubhubbub_publish.URL_BATCH_SIZE = 2
      pubsubhubbub_publish.publish(self.hub + '/batch',
                                   [self.feed, self.feed2, self.feed3])
    finally:
      pubsubhubbub_publish.URL_BATCH_SIZE = old
    self.assertEquals(2, REQUESTS)

  def testBadHubHostname(self):
    self.assertRaises(
        pubsubhubbub_publish.PublishError,
        pubsubhubbub_publish.publish,
        'http://asdf.does.not.resolve', self.feed)

  def testBadArgument(self):
    self.assertRaises(
        pubsubhubbub_publish.PublishError,
        pubsubhubbub_publish.publish,
        self.hub + '/fail', self.feed)

  def testBadHubUrl(self):
    self.assertRaises(
        pubsubhubbub_publish.PublishError,
        pubsubhubbub_publish.publish,
        'not://a.url.is.this', self.feed)

  def testNotFound(self):
    self.assertRaises(
        pubsubhubbub_publish.PublishError,
        pubsubhubbub_publish.publish,
        self.hub + '/unknown', self.feed)


if __name__ == '__main__':
  unittest.main()
