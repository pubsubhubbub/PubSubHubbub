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

"""Tests for the dos module."""

import logging
logging.basicConfig(format='%(levelname)-8s %(filename)s] %(message)s')
import os
import sys
import unittest

import testutil
testutil.fix_path()

from google.appengine.ext import webapp

import dos

################################################################################

class LimitTestBase(testutil.HandlerTestBase):
  """Base class for limit function tests."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.old_environ = os.environ.copy()
    os.environ['PATH_INFO'] = '/foobar_path'

  def tearDown(self):
    """Tears down the test hardness."""
    testutil.HandlerTestBase.tearDown(self)
    os.environ.clear()
    os.environ.update(self.old_environ)


class HeaderHandler(webapp.RequestHandler):

  # Rate limit by headers
  @dos.limit(count=3, period=10)
  def get(self):
    self.response.out.write('get success')

  # Rate limit by custom header
  @dos.limit(header='HTTP_FANCY_HEADER', count=3, period=10)
  def post(self):
    self.response.out.write('post success')


class HeaderTest(LimitTestBase):
  """Tests for limiting by only headers."""

  handler_class = HeaderHandler

  def testDefaultHeader(self):
    """Tests limits on a default header."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'
    for i in xrange(3):
      self.handle('get')
      self.assertEquals(200, self.response_code())
      self.assertEquals('get success', self.response_body())
    self.handle('get')
    self.assertEquals(503, self.response_code())

    # Different header value will not be limited.
    os.environ['REMOTE_ADDR'] = '10.1.1.4'
    self.handle('get')
    self.assertEquals(200, self.response_code())

  def testCustomHeader(self):
    """Tests limits on a default header."""
    header = 'HTTP_FANCY_HEADER'
    os.environ[header] = 'my cool header value'
    for i in xrange(3):
      self.handle('post')
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())
    self.handle('post')
    self.assertEquals(503, self.response_code())

    # Different header value will not be limited.
    os.environ['HTTP_FANCY_HEADER'] = 'something else'
    self.handle('post')
    self.assertEquals(200, self.response_code())

  def testHeaderMissing(self):
    """Tests when rate-limiting on a header that's missing."""
    # Should not allow more than three requests here, but
    # since there is no limit key, we let them all through.
    for i in xrange(4):
      self.handle('get')
      self.assertEquals(200, self.response_code())
      self.assertEquals('get success', self.response_body())


class ParamHandler(webapp.RequestHandler):

  # Limit by parameter
  @dos.limit(param='foo', header=None, count=3, period=10)
  def post(self):
    self.response.out.write('post success')


class ParamTest(LimitTestBase):
  """Tests for limiting by only parameters."""

  handler_class = ParamHandler

  def testParam(self):
    """Tests limits on a parameter."""
    for i in xrange(3):
      self.handle('post', ('foo', 'meep'))
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())
    self.handle('post', ('foo', 'meep'))
    self.assertEquals(503, self.response_code())

    # Different parameter value will not be limited.
    self.handle('post', ('foo', 'wooh'))
    self.assertEquals(200, self.response_code())

  def testParamMissing(self):
    """Tests when rate-limiting on a parameter that's missing."""
    # Should not allow more than three requests here, but
    # since there is no limit key, we let them all through.
    for i in xrange(4):
      self.handle('post')
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())


class ParamAndHeaderHandler(webapp.RequestHandler):

  # Limit by headers and params
  @dos.limit(param='foo', count=3, period=10)
  def post(self):
    self.response.out.write('post success')


class ParamAndHeaderTest(LimitTestBase):
  """Tests for limiting by parameters and headers."""

  handler_class = ParamAndHeaderHandler

  def testHeaderAndParam(self):
    """Tests when a header and parameter are limited."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'
    for i in xrange(3):
      self.handle('post', ('foo', 'meep'))
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())
    self.handle('post', ('foo', 'meep'))
    self.assertEquals(503, self.response_code())

    # Different header *or* parmaeter values will not be limited.
    self.handle('post', ('foo', 'stuff'))
    self.assertEquals(200, self.response_code())

    os.environ['REMOTE_ADDR'] = '10.1.1.4'
    self.handle('post', ('foo', 'meep'))
    self.assertEquals(200, self.response_code())

  def testHeaderMissing(self):
    """Tests when the header should be there too but isn't."""
    # Should not allow more than three requests here, but
    # since there is no limit key, we let them all through.
    for i in xrange(4):
      self.handle('post', ('foo', 'meep'))
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())

  def testParamMissing(self):
    """Tests when the parameter should be there too but isn't."""
    # Should not allow more than three requests here, but
    # since there is no limit key, we let them all through.
    os.environ['REMOTE_ADDR'] = '10.1.1.4'
    for i in xrange(4):
      self.handle('post')
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())

  def testBothMissing(self):
    """Tests when the header and parameter are missing."""
    # Should not allow more than three requests here, but
    # since there is no limit key, we let them all through.
    for i in xrange(4):
      self.handle('post')
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())


class MethodsAndUrlsHandler(webapp.RequestHandler):

  @dos.limit(count=3, period=10)
  def get(self):
    self.response.out.write('get success')

  @dos.limit(count=3, period=10)
  def post(self):
    self.response.out.write('post success')


class MethodsAndUrlsTest(LimitTestBase):
  """Tests for limiting across various methods and URLs."""

  handler_class = MethodsAndUrlsHandler

  def testMethods(self):
    """Tests that methods are limited separately."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'
    for i in xrange(3):
      self.handle('post')
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())
    self.handle('post')
    self.assertEquals(503, self.response_code())

    # Different method still works.
    self.handle('get')
    self.assertEquals(200, self.response_code())

  def testUrls(self):
    """Tests that limiting for the same verb on different URLs works."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'
    for i in xrange(3):
      self.handle('post')
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())
    self.handle('post')
    self.assertEquals(503, self.response_code())

    # Different path still works.
    os.environ['PATH_INFO'] = '/other_path'
    self.handle('post')
    self.assertEquals(200, self.response_code())


class ErrorParamsHandler(webapp.RequestHandler):

  # Alternate error code
  @dos.limit(count=0, period=1, error_code=409, retry_after=99)
  def get(self):
    self.response.out.write('get success')

  # No retry-after time
  @dos.limit(count=0, period=1, retry_after=None)
  def post(self):
    self.response.out.write('post success')

  # Defaults
  @dos.limit(count=0, period=1)
  def put(self):
    self.response.out.write('put success')


class ErrorParamsTest(LimitTestBase):
  """Tests the error and retry paramters"""

  handler_class = ErrorParamsHandler

  def testDefaultRetryAmount(self):
    """Tests the supplied retry amount is valid."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'
    self.handle('put')
    self.assertEquals(503, self.response_code())
    self.assertEquals('120', self.response_headers().get('Retry-After'))

  def testCustomErrorCode(self):
    """Tests when a custom error code and retry time are specified."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'
    self.handle('get')
    self.assertEquals(409, self.response_code())
    self.assertEquals('99', self.response_headers().get('Retry-After'))

  def testNoRetryTime(self):
    """Tests when no retry time should be returned."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'
    self.handle('post')
    self.assertEquals(503, self.response_code())
    self.assertEquals(None, self.response_headers().get('Retry-After'))


class ConfigErrorTest(unittest.TestCase):
  """Tests various limit configuration errors."""

  def testNoKeyError(self):
    """Tests when there is no limiting key to derive."""
    self.assertRaises(
        dos.ConfigError,
        dos.limit,
        header=None,
        param=None)

  def testNegativeCount(self):
    """Tests when the count is less than zero."""
    self.assertRaises(
        dos.ConfigError,
        dos.limit,
        count=-1,
        period=1)

  def testZeroPeriod(self):
    """Tests when the count is less than zero."""
    self.assertRaises(
        dos.ConfigError,
        dos.limit,
        count=1,
        period=0)

  def testParamForDifferentVerb(self):
    """Tests trying to rate limit a ."""
    def put():
      pass
    wrapper = dos.limit(param='okay', count=1, period=1)
    self.assertRaises(dos.ConfigError, wrapper, put)


class MemcacheDetailsHandler(webapp.RequestHandler):

  @dos.limit(count=0, period=5.234)
  def post(self):
    self.response.out.write('post success')


class MemcacheDetailTest(LimitTestBase):
  """Tests for various memcache details and failures."""

  handler_class = MemcacheDetailsHandler

  def setUp(self):
    """Sets up the test harness."""
    LimitTestBase.setUp(self)
    os.environ['REMOTE_ADDR'] = '10.1.1.4'
    self.expected_key = 'POST /foobar_path REMOTE_ADDR=10.1.1.4'
    self.expected_incr = []
    self.expected_add = []
    self.old_incr = dos.memcache.incr
    self.old_add = dos.memcache.add

    def incr(key):
      self.assertEquals(self.expected_key, key)
      return self.expected_incr.pop(0)
    dos.memcache.incr = incr

    def add(key, value, time=None):
      self.assertEquals(self.expected_key, key)
      self.assertEquals(1, value)
      self.assertEquals(5.234, time)
      return self.expected_add.pop(0)
    dos.memcache.add = add

  def tearDown(self):
    """Tears down the test harness."""
    LimitTestBase.tearDown(self)
    self.assertEquals(0, len(self.expected_incr))
    self.assertEquals(0, len(self.expected_add))
    dos.memcache.incr = self.old_incr
    dos.memcache.add = self.old_add

  def testIncrFailure(self):
    """Tests when the initial increment fails."""
    self.expected_incr.append(None)
    self.expected_add.append(True)
    self.handle('post')
    self.assertEquals(503, self.response_code())

  def testIncrAndAddFailure(self):
    """Tests when the initial increment and the following add fail."""
    self.expected_incr.append(None)
    self.expected_add.append(False)
    self.expected_incr.append(14)
    self.handle('post')
    self.assertEquals(503, self.response_code())

  def testCompleteFailure(self):
    """Tests when all memcache calls fail."""
    self.expected_incr.append(None)
    self.expected_add.append(False)
    self.expected_incr.append(None)
    self.handle('post')
    self.assertEquals(200, self.response_code())
    self.assertEquals('post success', self.response_body())


class WhiteListHandler(webapp.RequestHandler):

  @dos.limit(count=0, period=1,
             header_whitelist=set(['10.1.1.5', '10.1.1.6']))
  def get(self):
    self.response.out.write('get success')

  @dos.limit(param='foobar', header=None, count=0, period=1,
             param_whitelist=set(['meep', 'stuff']))
  def post(self):
    self.response.out.write('post success')


class WhiteListTest(LimitTestBase):
  """Tests for white-listing."""

  handler_class = WhiteListHandler

  def testHeaderWhitelist(self):
    """Tests white-lists for headers."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'
    self.handle('get')
    self.assertEquals(503, self.response_code())

    for addr in ('10.1.1.5', '10.1.1.6'):
      os.environ['REMOTE_ADDR'] = addr
      self.handle('get')
      self.assertEquals(200, self.response_code())
      self.assertEquals('get success', self.response_body())

  def testParameterWhitelist(self):
    """Tests white-lists for parameters."""
    self.handle('post', ('foobar', 'zebra'))
    self.assertEquals(503, self.response_code())

    for value in ('meep', 'stuff'):
      self.handle('post', ('foobar', value))
      self.assertEquals(200, self.response_code())
      self.assertEquals('post success', self.response_body())


class LayeredHandler(webapp.RequestHandler):

  @dos.limit(count=3, period=1)
  @dos.limit(header=None, param='stuff', count=0, period=1)
  def get(self):
    self.response.out.write('get success')


class LayeringTest(LimitTestBase):
  """Tests that dos limits can be layered."""

  handler_class = LayeredHandler

  def testLayering(self):
    """Tests basic layering."""
    os.environ['REMOTE_ADDR'] = '10.1.1.3'

    # First request works normally, limiting by IP.
    self.handle('get')
    self.assertEquals(200, self.response_code())
    self.assertEquals('get success', self.response_body())

    # Next request uses param and is blocked.
    self.handle('get', ('stuff', 'meep'))
    self.assertEquals(503, self.response_code())

    # Next request without param is allowed, following one is blocked.
    self.handle('get')
    self.assertEquals(200, self.response_code())
    self.assertEquals('get success', self.response_body())
    self.handle('get')
    self.assertEquals(503, self.response_code())

################################################################################

if __name__ == '__main__':
  unittest.main()
