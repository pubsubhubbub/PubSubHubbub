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

import cProfile
import gc
import logging
logging.basicConfig(format='%(levelname)-8s %(filename)s] %(message)s')
import os
import random
import sys
import unittest

import testutil
testutil.fix_path()

from google.appengine.api import memcache
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

class GetUrlDomainTest(unittest.TestCase):
  """Tests for the get_url_domain function."""

  def testDomain(self):
    """Tests good domain names."""
    # No subdomain
    self.assertEquals(
        'example.com',
        dos.get_url_domain('http://example.com/foo/bar?meep=stuff#asdf'))
    # One subdomain
    self.assertEquals(
        'www.example.com',
        dos.get_url_domain('http://www.example.com/foo/bar?meep=stuff#asdf'))
    # Many subdomains
    self.assertEquals(
        '1.2.3.many.sub.example.com',
        dos.get_url_domain('http://1.2.3.many.sub.example.com/'))
    # Domain with no trailing path
    self.assertEquals(
        'www.example.com',
        dos.get_url_domain('http://www.example.com'))

  def testDomainExceptions(self):
    """Tests that some URLs may use more than the domain suffix."""
    self.assertEquals(
        'blogspot.com',
        dos.get_url_domain('http://example.blogspot.com/this-is?some=test'))

  def testIP(self):
    """Tests IP addresses."""
    self.assertEquals(
        '192.168.1.1',
        dos.get_url_domain('http://192.168.1.1/foo/bar?meep=stuff#asdf'))
    # No trailing path
    self.assertEquals(
        '192.168.1.1',
        dos.get_url_domain('http://192.168.1.1'))

  def testOther(self):
    """Tests anything that's not IP- or domain-like."""
    self.assertEquals(
        'localhost',
        dos.get_url_domain('http://localhost/foo/bar?meep=stuff#asdf'))
    # No trailing path
    self.assertEquals(
        'localhost',
        dos.get_url_domain('http://localhost'))

  def testBadUrls(self):
    """Tests URLs that are bad."""
    self.assertEquals('bad_url',
        dos.get_url_domain('this is bad'))
    self.assertEquals('bad_url',    
        dos.get_url_domain('example.com/foo/bar?meep=stuff#asdf'))
    self.assertEquals('bad_url',    
        dos.get_url_domain('example.com'))
    self.assertEquals('bad_url',    
        dos.get_url_domain('//example.com'))
    self.assertEquals('bad_url',
        dos.get_url_domain('/myfeed.atom'))
    self.assertEquals('bad_url',
        dos.get_url_domain('192.168.0.1/foobar'))
    self.assertEquals('bad_url',
        dos.get_url_domain('192.168.0.1'))

  def testCaching(self):
    """Tests that cache eviction works properly."""
    dos._DOMAIN_CACHE.clear()
    old_size = dos.DOMAIN_CACHE_SIZE
    try:
      dos.DOMAIN_CACHE_SIZE = 2
      dos._DOMAIN_CACHE['http://a.example.com/stuff'] = 'a.example.com'
      dos._DOMAIN_CACHE['http://b.example.com/stuff'] = 'b.example.com'
      dos._DOMAIN_CACHE['http://c.example.com/stuff'] = 'c.example.com'
      self.assertEquals(3, len(dos._DOMAIN_CACHE))

      # Old cache entries are hit:
      self.assertEquals('c.example.com',
                        dos.get_url_domain('http://c.example.com/stuff'))
      self.assertEquals(3, len(dos._DOMAIN_CACHE))

      # New cache entries clear the contents.
      self.assertEquals('d.example.com',
                        dos.get_url_domain('http://d.example.com/stuff'))
      self.assertEquals(1, len(dos._DOMAIN_CACHE))
    finally:
      dos.DOMAIN_CACHE_SIZE = old_size

################################################################################

class OffsetOrAddTest(unittest.TestCase):
  """Tests for the offset_or_add function."""

  def setUp(self):
    """Sets up the test harness."""
    self.offsets = None
    self.offset_multi = lambda *a, **k: self.offsets.next()(*a, **k)
    self.adds = None
    self.add_multi = lambda *a, **k: self.adds.next()(*a, **k)

  def testAlreadyExist(self):
    """Tests when the keys already exist and can just be added to."""
    def offset_multi():
      yield lambda *a, **k: {'one': 2, 'three': 4}
    self.offsets = offset_multi()

    self.assertEquals(
        {'one': 2, 'three': 4},
        dos.offset_or_add({'blue': 15, 'red': 10}, 5,
                          offset_multi=self.offset_multi,
                          add_multi=self.add_multi))

  def testKeysAdded(self):
    """Tests when some keys need to be re-added."""
    def offset_multi():
      yield lambda *a, **k: {'one': None, 'three': 4, 'five': None}
    self.offsets = offset_multi()

    def add_multi():
      def run(adds, **kwargs):
        self.assertEquals({'one': 5, 'five': 10}, adds)
        return []
      yield run
    self.adds = add_multi()

    self.assertEquals(
        {'one': 5, 'three': 4, 'five': 10},
        dos.offset_or_add({'one': 5, 'three': 0, 'five': 10}, 5,
                          offset_multi=self.offset_multi,
                          add_multi=self.add_multi))

  def testAddsRace(self):
    """Tests when re-adding keys is a race that is lost."""
    def offset_multi():
      yield lambda *a, **k: {'one': None, 'three': 4, 'five': None}
      yield lambda *a, **k: {'one': 5, 'five': 10}
    self.offsets = offset_multi()

    def add_multi():
      def run(adds, **kwargs):
        self.assertEquals({'one': 5, 'five': 10}, adds)
        return ['one', 'five']
      yield run
    self.adds = add_multi()

    self.assertEquals(
        {'one': 5, 'three': 4, 'five': 10},
        dos.offset_or_add({'one': 5, 'three': 0, 'five': 10}, 5,
                          offset_multi=self.offset_multi,
                          add_multi=self.add_multi))

  def testOffsetsFailAfterRace(self):
    """Tests when the last offset call fails."""
    def offset_multi():
      yield lambda *a, **k: {'one': None, 'three': 4, 'five': None}
      yield lambda *a, **k: {'one': None, 'five': None}
    self.offsets = offset_multi()

    def add_multi():
      def run(adds, **kwargs):
        self.assertEquals({'one': 5, 'five': 10}, adds)
        return ['one', 'five']
      yield run
    self.adds = add_multi()

    self.assertEquals(
        {'one': None, 'three': 4, 'five': None},
        dos.offset_or_add({'one': 5, 'three': 0, 'five': 10}, 5,
                          offset_multi=self.offset_multi,
                          add_multi=self.add_multi))

################################################################################

class SamplerTest(unittest.TestCase):
  """Tests for the MultiSampler class."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.domainA = 'mydomain.com'
    self.domainB = 'example.com'
    self.domainC = 'other.com'
    self.domainD = 'meep.com'
    self.url1 = 'http://mydomain.com/stuff/meep'
    self.url2 = 'http://example.com/some-path?a=b'
    self.url3 = 'http://example.com'
    self.url4 = 'http://other.com/relative'
    self.url5 = 'http://meep.com/another-one'
    self.all_urls = [self.url1, self.url2, self.url3, self.url4, self.url5]

    self.randrange_results = []
    self.fake_randrange = lambda value: self.randrange_results.pop(0)

    self.random_results = []
    self.fake_random = lambda: self.random_results.pop(0)

    self.gettime_results = []
    self.fake_gettime = lambda: self.gettime_results.pop(0)

  def verify_sample(self,
                    results,
                    key,
                    expected_count,
                    expected_frequency,
                    expected_average=1,
                    expected_min=1,
                    expected_max=1):
    """Verifies a sample key is present in the results.

    Args:
      results: SampleResult object.
      key: String key of the sample to test.
      expected_count: How many samples should be present in the results.
      expected_frequency: The frequency of this single key.
      expected_average: Expected average value across samples of this key.
      expected_min: Expected minimum value across samples of this key.
      expected_max: Expected maximum value across samples of this key.

    Raises:
      AssertionError if any of the expectations are not met.
    """
    self.assertEquals(expected_count, results.get_count(key))
    self.assertTrue(
        -0.001 < (expected_frequency - results.get_frequency(key)) < 0.001,
        'Difference %f - %f = %f' % (
            expected_frequency, results.get_frequency(key),
            expected_frequency - results.get_frequency(key)))
    self.assertTrue(
        -0.001 < (expected_average - results.get_average(key)) < 0.001,
        'Difference %f - %f  %f' % (
            expected_average, results.get_average(key),
            expected_average - results.get_average(key)))
    self.assertEquals(expected_min, results.get_min(key))
    self.assertEquals(expected_max, results.get_max(key))

  def verify_no_sample(self, results, key):
    """Verifies a sample key is not present in the results.

    Args:
      results: SampleResult object.
      key: String key of the sample to test.

    Raises:
      AssertionError if the key is present.
    """
    self.assertEquals(0, len(results.get_samples(key)))

  def testSingleAlways(self):
    """Tests single-config sampling when the sampling rate is 100%."""
    config = dos.ReservoirConfig(
        'always',
        period=300,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    reporter.set(self.url4, config)
    reporter.set(self.url5, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(5, results.total_samples)
    self.assertEquals(5, results.unique_samples)
    self.verify_sample(results, self.domainA, 1, 0.1)
    self.verify_sample(results, self.domainB, 2, 0.2)
    self.verify_sample(results, self.domainC, 1, 0.1)
    self.verify_sample(results, self.domainD, 1, 0.1)

    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(10, results.total_samples)
    self.assertEquals(10, results.unique_samples)
    self.verify_sample(results, self.domainA, 2, 0.2)
    self.verify_sample(results, self.domainB, 4, 0.4)
    self.verify_sample(results, self.domainC, 2, 0.2)
    self.verify_sample(results, self.domainD, 2, 0.2)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(11, results.total_samples)
    self.assertEquals(11, results.unique_samples)
    self.verify_sample(results, self.domainA, 3, 0.3)
    self.verify_sample(results, self.domainB, 4, 0.4)
    self.verify_sample(results, self.domainC, 2, 0.2)
    self.verify_sample(results, self.domainD, 2, 0.2)

  def testSingleOverwrite(self):
    """Tests when the number of slots is lower than the sample count."""
    config = dos.ReservoirConfig(
        'always',
        period=300,
        rate=1,
        samples=2,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    # Writes samples index 0 and 1, then overwrites index 1 again with
    # a URL in the same domain.
    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    self.gettime_results.extend([0, 1])
    self.randrange_results.extend([1])
    sampler.sample(reporter, randrange=self.fake_randrange)
    results = sampler.get(config)
    self.assertEquals(3, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_sample(results, self.domainA, 1, 1.5)
    self.verify_sample(results, self.domainB, 1, 1.5)

    # Overwrites the sample at index 0, skewing all results towards the
    # domain from index 1.
    reporter = dos.Reporter()
    reporter.set(self.url3, config)
    self.gettime_results.extend([0, 1])
    self.randrange_results.extend([0])
    sampler.sample(reporter, randrange=self.fake_randrange)
    results = sampler.get(config)
    self.assertEquals(4, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_sample(results, self.domainB, 2, 4.0)
    self.verify_no_sample(results, self.domainA)

    # Now a sample outside the range won't replace anything.
    self.gettime_results.extend([0, 1])
    self.randrange_results.extend([3])
    sampler.sample(reporter, randrange=self.fake_randrange)
    results = sampler.get(config)
    self.assertEquals(5, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_sample(results, self.domainB, 2, 5.0)
    self.verify_no_sample(results, self.domainA)

  def testSingleSampleRate(self):
    """Tests when the sampling rate is less than 1."""
    config = dos.ReservoirConfig(
        'always',
        period=300,
        rate=0.2,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    reporter.set(self.url4, config)
    reporter.set(self.url5, config)
    self.gettime_results.extend([0, 10])
    self.random_results.extend([0.25, 0.199, 0.1, 0, 0.201])
    sampler.sample(reporter, getrandom=self.fake_random)
    results = sampler.get(config)
    self.assertEquals(3, results.total_samples)
    self.assertEquals(3, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainD)
    self.verify_sample(results, self.domainB, 2,
                       (1.0/0.2) * (2.0/3.0) * (3.0/10.0))
    self.verify_sample(results, self.domainC, 1,
                       (1.0/0.2) * (1.0/3.0) * (3.0/10.0))

  def testSingleDoubleSampleRemoved(self):
    """Tests when the same sample key is set twice and one is skipped.

    Setting the value twice should just overwite the previous value for a key,
    but we store the keys in full order (with dupes) for simpler tests. This
    ensures that incorrectly using the sampler with multiple sets won't barf.
    """
    config = dos.ReservoirConfig(
        'always',
        period=300,
        rate=0.2,
        samples=4,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    reporter.set(self.url4, config)
    reporter.set(self.url5, config)
    self.gettime_results.extend([0, 10])
    self.randrange_results.extend([0])
    self.random_results.extend([0.25, 0.199, 0.1, 0, 0.3, 0.3])
    sampler.sample(reporter, getrandom=self.fake_random)
    results = sampler.get(config)
    self.assertEquals(3, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainC)
    self.verify_no_sample(results, self.domainD)
    self.verify_sample(results, self.domainB, 2,
                       (1.0/0.2) * (2.0/2.0) * (3.0/10.0))

  def testSingleSampleRateReplacement(self):
    """Tests when the sample rate is < 1 and slots are overwritten."""
    config = dos.ReservoirConfig(
        'always',
        period=300,
        rate=0.2,
        samples=2,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    reporter.set(self.url4, config)
    self.gettime_results.extend([0, 10])
    self.randrange_results.extend([1])
    self.random_results.extend([0.25, 0.199, 0.1, 0])
    sampler.sample(reporter, getrandom=self.fake_random)
    results = sampler.get(config)
    self.assertEquals(3, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainD)
    self.verify_sample(results, self.domainB, 1,
                       (1.0/0.2) * (1.0/2.0) * (3.0/10.0))
    self.verify_sample(results, self.domainC, 1,
                       (1.0/0.2) * (1.0/2.0) * (3.0/10.0))

  def testSingleSampleValues(self):
    """Tests various samples with expected values."""
    config = dos.ReservoirConfig(
        'always',
        period=300,
        rate=0.2,
        samples=4,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config, 5)
    reporter.set(self.url1, config, 20) # in
    reporter.set(self.url2, config, 10) # in
    reporter.set(self.url2 + '&more=true', config, 25) # in
    reporter.set(self.url3, config, 20) # in
    reporter.set(self.url4, config, 40) # in
    reporter.set(self.url5, config, 60)
    self.gettime_results.extend([0, 10])
    self.randrange_results.extend([0])
    self.random_results.extend([0.25, 0.199, 0.1, 0, 0, 0.1, 0.3])
    sampler.sample(reporter,
                   randrange=self.fake_randrange,
                   getrandom=self.fake_random)
    results = sampler.get(config)
    self.assertEquals(5, results.total_samples)
    self.assertEquals(4, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainD)
    self.verify_sample(results, self.domainB, 3,
                       (1.0/0.2) * (3.0/4.0) * (5.0/10.0),
                       expected_average=18.333,
                       expected_min=10,
                       expected_max=25)
    self.verify_sample(results, self.domainC, 1,
                       (1.0/0.2) * (1.0/4.0) * (5.0/10.0),
                       expected_average=40,
                       expected_min=40,
                       expected_max=40)

  def testResetTimestamp(self):
    """Tests resetting the timestamp after the period elapses."""
    config = dos.ReservoirConfig(
        'always',
        period=10,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    self.gettime_results.extend([0, 5])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(1, results.total_samples)
    self.assertEquals(1, results.unique_samples)
    self.verify_sample(results, self.domainA, 1, 1.0 / 5)
    self.verify_no_sample(results, self.domainB)
    self.verify_no_sample(results, self.domainC)
    self.verify_no_sample(results, self.domainD)

    reporter = dos.Reporter()
    reporter.set(self.url2, config)
    self.gettime_results.extend([15, 16])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(1, results.total_samples)
    self.assertEquals(1, results.unique_samples)
    self.verify_sample(results, self.domainB, 1, 1.0)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainC)
    self.verify_no_sample(results, self.domainD)

  def testSingleUnicodeKey(self):
    """Tests when a sampling key is unicode.

    Keys must be UTF-8 encoded because the memcache API will do this for us
    (and break) if we don't.
    """
    config = dos.ReservoirConfig(
        'always',
        period=300,
        samples=10000,
        by_url=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    key = u'this-breaks-stuff\u30d6\u30ed\u30b0\u8846'
    key_utf8 = key.encode('utf-8')
    reporter.set(key, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(1, results.total_samples)
    self.assertEquals(1, results.unique_samples)
    self.verify_sample(results, key_utf8, 1, 0.1)

  def testMultiple(self):
    """Tests multiple configs being applied together."""
    config1 = dos.ReservoirConfig(
        'first',
        period=300,
        samples=10000,
        by_domain=True)
    config2 = dos.ReservoirConfig(
        'second',
        period=300,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config1, config2], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config1)
    reporter.set(self.url2, config1)
    reporter.set(self.url3, config1)
    reporter.set(self.url4, config1)
    reporter.set(self.url5, config1)
    reporter.set(self.url1, config2, 5)
    reporter.set(self.url2, config2, 5)
    reporter.set(self.url3, config2, 5)
    reporter.set(self.url4, config2, 5)
    reporter.set(self.url5, config2, 5)
    self.gettime_results.extend([0, 10, 10])
    sampler.sample(reporter)

    results1 = sampler.get(config1)
    self.assertEquals(5, results1.total_samples)
    self.assertEquals(5, results1.unique_samples)
    self.verify_sample(results1, self.domainA, 1, 0.1)
    self.verify_sample(results1, self.domainB, 2, 0.2)
    self.verify_sample(results1, self.domainC, 1, 0.1)
    self.verify_sample(results1, self.domainD, 1, 0.1)

    results2 = sampler.get(config2)
    self.assertEquals(5, results2.total_samples)
    self.assertEquals(5, results2.unique_samples)
    self.verify_sample(results2, self.domainA, 1, 0.1,
                       expected_max=5,
                       expected_min=5,
                       expected_average=5)
    self.verify_sample(results2, self.domainB, 2, 0.2,
                       expected_max=5,
                       expected_min=5,
                       expected_average=5)
    self.verify_sample(results2, self.domainC, 1, 0.1,
                       expected_max=5,
                       expected_min=5,
                       expected_average=5)
    self.verify_sample(results2, self.domainD, 1, 0.1,
                       expected_max=5,
                       expected_min=5,
                       expected_average=5)

  def testGetSingleKey(self):
    """Tests getting the stats for a single key."""
    config = dos.ReservoirConfig(
        'single-sample',
        period=300,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    reporter.set(self.url3 + '&okay=1', config)
    reporter.set(self.url3 + '&okay=2', config)
    reporter.set(self.url3 + '&okay=3', config)
    reporter.set(self.url3 + '&okay=4', config)
    reporter.set(self.url4, config)
    reporter.set(self.url5, config)
    self.gettime_results.extend([0, 10, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(9, results.total_samples)
    self.assertEquals(9, results.unique_samples)
    self.verify_sample(results, self.domainA, 1, 0.1)
    self.verify_sample(results, self.domainB, 6, 0.6)
    self.verify_sample(results, self.domainC, 1, 0.1)
    self.verify_sample(results, self.domainD, 1, 0.1)

    results = sampler.get(config, self.url2)
    self.assertEquals(6, results.total_samples)
    self.assertEquals(6, results.unique_samples)
    self.verify_sample(results, self.domainB, 6, 0.6)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainC)
    self.verify_no_sample(results, self.domainD)

  def testCountLost(self):
    """Tests when the count variable disappears between samples."""
    config = dos.ReservoirConfig(
        'lost_count',
        period=300,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(2, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_no_sample(results, self.domainC)
    self.verify_no_sample(results, self.domainD)
    self.verify_sample(results, self.domainA, 1, 0.1)
    self.verify_sample(results, self.domainB, 1, 0.1)

    memcache.delete('lost_count:by_domain:counter')
    reporter = dos.Reporter()
    reporter.set(self.url4, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(1, results.total_samples)

    # Two samples found because we're still in the same period tolerance.
    # Sample at index 0 will be overwritten with the new entry, meaning
    # domain A is gone.
    self.assertEquals(2, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainD)
    self.verify_sample(results, self.domainB, 1, 0.05)
    self.verify_sample(results, self.domainC, 1, 0.05)

  def testStampLost(self):
    """Tests when the start timestamp is lost between samples."""
    config = dos.ReservoirConfig(
        'lost_stamp',
        period=300,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(2, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_no_sample(results, self.domainC)
    self.verify_no_sample(results, self.domainD)
    self.verify_sample(results, self.domainA, 1, 0.1)
    self.verify_sample(results, self.domainB, 1, 0.1)

    memcache.delete('lost_stamp:by_domain:start_time')
    reporter = dos.Reporter()
    reporter.set(self.url4, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)
    results = sampler.get(config)
    self.assertEquals(1, results.total_samples)

    # Just like losing the count, old samples found because we're still in the
    # same period tolerance. Sample at index 0 will be overwritten with the new
    # entry, meaning domain A is gone.
    self.assertEquals(2, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainD)
    self.verify_sample(results, self.domainB, 1, 0.05)
    self.verify_sample(results, self.domainC, 1, 0.05)

  def testSamplesLost(self):
    """Tests when some unique samples were evicted."""
    config = dos.ReservoirConfig(
        'lost_sample',
        period=300,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    reporter.set(self.url4, config)
    reporter.set(self.url5, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)

    memcache.delete_multi([
      'lost_sample:by_domain:0',
      'lost_sample:by_domain:1',
      'lost_sample:by_domain:2',
    ])

    results = sampler.get(config)
    self.assertEquals(5, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainB)
    self.verify_sample(results, self.domainC, 1, 0.25)
    self.verify_sample(results, self.domainD, 1, 0.25)

  def testBeforePeriod(self):
    """Tests when the samples retrieved are too old."""
    config = dos.ReservoirConfig(
        'old_samples',
        period=10,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    reporter.set(self.url4, config)
    reporter.set(self.url5, config)
    self.gettime_results.extend([20, 40])
    sampler.sample(reporter)

    memcache.set('old_samples:by_domain:start_time', 0)
    results = sampler.get(config)
    self.assertEquals(5, results.total_samples)
    self.assertEquals(0, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainB)
    self.verify_no_sample(results, self.domainC)
    self.verify_no_sample(results, self.domainD)

  def testBadSamples(self):
    """Tests when getting samples with memcache values that are bad."""
    config = dos.ReservoirConfig(
        'bad_samples',
        period=10,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config)
    reporter.set(self.url2, config)
    reporter.set(self.url3, config)
    reporter.set(self.url4, config)
    reporter.set(self.url5, config)
    self.gettime_results.extend([0, 10])
    sampler.sample(reporter)

    # Totaly bad
    memcache.set('bad_samples:by_domain:0', 'garbage')
    # Bad value.
    memcache.set('bad_samples:by_domain:1',
                 '%s:\0\0\0\1:' % self.domainB)
    # Bad when.
    memcache.set('bad_samples:by_domain:2',
                 '%s::\0\0\0\1' % self.domainB)

    results = sampler.get(config)
    self.assertEquals(5, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_no_sample(results, self.domainA)
    self.verify_no_sample(results, self.domainB)
    self.verify_sample(results, self.domainC, 1, 0.25)
    self.verify_sample(results, self.domainD, 1, 0.25)

  def testGetChain(self):
    """Tests getting results from multiple configs in a single call."""
    config1 = dos.ReservoirConfig(
        'first',
        period=300,
        rate=1,
        samples=10000,
        by_domain=True)
    config2 = dos.ReservoirConfig(
        'second',
        period=300,
        rate=1,
        samples=10000,
        by_url=True)
    sampler = dos.MultiSampler([config1, config2], gettime=self.fake_gettime)

    reporter = dos.Reporter()
    reporter.set(self.url1, config1)
    reporter.set(self.url2, config1)
    reporter.set(self.url3, config1)
    reporter.set(self.url4, config1)
    reporter.set(self.url5, config1)
    reporter.set(self.url1, config2)
    reporter.set(self.url2, config2)
    reporter.set(self.url3, config2)
    reporter.set(self.url4, config2)
    reporter.set(self.url5, config2)
    self.gettime_results.extend([0, 10, 10, 10, 10])
    sampler.sample(reporter)
    result_iter = sampler.get_chain(config1, config2)

    # Results for config1
    results = result_iter.next()
    self.assertEquals(5, results.total_samples)
    self.assertEquals(5, results.unique_samples)
    self.verify_sample(results, self.domainA, 1, 0.1)
    self.verify_sample(results, self.domainB, 2, 0.2)
    self.verify_sample(results, self.domainC, 1, 0.1)
    self.verify_sample(results, self.domainD, 1, 0.1)

    # Results for config2
    results = result_iter.next()
    self.assertEquals(5, results.total_samples)
    self.assertEquals(5, results.unique_samples)
    self.verify_sample(results, self.url1, 1, 0.1)
    self.verify_sample(results, self.url2, 1, 0.1)
    self.verify_sample(results, self.url3, 1, 0.1)
    self.verify_sample(results, self.url4, 1, 0.1)
    self.verify_sample(results, self.url5, 1, 0.1)

    # Single key test
    result_iter = sampler.get_chain(
        config1, config2,
        single_key=self.url2)

    # Results for config1
    results = result_iter.next()
    self.assertEquals(2, results.total_samples)
    self.assertEquals(2, results.unique_samples)
    self.verify_sample(results, self.domainB, 2, 0.2)

    # Results for config2
    results = result_iter.next()
    self.assertEquals(1, results.total_samples)
    self.assertEquals(1, results.unique_samples)
    self.verify_sample(results, self.url2, 1, 0.1)


  def testConfig(self):
    """Tests config validation."""
    # Bad name.
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        '',
        period=10,
        samples=10,
        by_domain=True)

    # Bad period.
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=0,
        samples=10,
        by_domain=True)
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=-1,
        samples=10,
        by_domain=True)
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period='bad',
        samples=10,
        by_domain=True)

    # Bad samples.
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=0,
        by_domain=True)
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=-1,
        by_domain=True)
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples='bad',
        by_domain=True)

    # Bad domain/url combo.
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=10,
        by_domain=True,
        by_url=True)
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=10,
        by_domain=False,
        by_url=False)

    # Bad rate.
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=10,
        rate=-1,
        by_domain=True)
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=10,
        rate=1.1,
        by_domain=True)
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=10,
        rate='bad',
        by_domain=True)

    # Bad tolerance.
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=10,
        tolerance=-1,
        by_domain=True)
    self.assertRaises(
        dos.ConfigError,
        dos.ReservoirConfig,
        'my name',
        period=10,
        samples=10,
        tolerance='bad',
        by_domain=True)

  def testSampleProfile(self):
    """Profiles the sample method with lots of data."""
    print 'Tracked objects start',len(gc.get_objects())
    config = dos.ReservoirConfig(
        'testing',
        period=10,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config])
    reporter = dos.Reporter()
    fake_urls = ['http://example-%s.com/meep' % i
                 for i in xrange(100)]
    for i in xrange(100000):
      reporter.set(random.choice(fake_urls), config, random.randint(0, 10000))
    del fake_urls
    gc.collect()
    dos._DOMAIN_CACHE.clear()

    gc.disable()
    gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)
    try:
      # Swap the two following lines to profile memory vs. CPU
      sampler.sample(reporter)
      #cProfile.runctx('sampler.sample(reporter)', globals(), locals())
      memcache.flush_all()  # Clear the string references
      print 'Tracked objects before collection', len(gc.get_objects())
      dos._DOMAIN_CACHE.clear()
      del reporter
      del sampler
    finally:
      print 'Unreachable', gc.collect()
      print 'Tracked objects after collection', len(gc.get_objects())
      gc.set_debug(0)
      gc.enable()

  def testGetProfile(self):
    """Profiles the get method when there's lots of data."""
    print 'Tracked objects start',len(gc.get_objects())
    config = dos.ReservoirConfig(
        'testing',
        period=10,
        rate=1,
        samples=10000,
        by_domain=True)
    sampler = dos.MultiSampler([config])
    reporter = dos.Reporter()
    fake_urls = ['http://example-%s.com/meep' % i
                 for i in xrange(100)]
    for i in xrange(100000):
      reporter.set(random.choice(fake_urls), config, random.randint(0, 10000))
    del fake_urls
    dos._DOMAIN_CACHE.clear()
    gc.collect()
    sampler.sample(reporter)

    gc.disable()
    gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)
    try:
      # Swap the two following lines to profile memory vs. CPU
      result = sampler.get(config)
      #cProfile.runctx('result = sampler.get(config)', globals(), locals())
      memcache.flush_all()  # Clear the string references
      print 'Tracked objects before collection', len(gc.get_objects())
      try:
        del locals()['result']
        del result
      except:
        pass
      dos._DOMAIN_CACHE.clear()
      del reporter
      del sampler
    finally:
      print 'Unreachable', gc.collect()
      print 'Tracked objects after collection', len(gc.get_objects())
      gc.set_debug(0)
      gc.enable()

################################################################################

class UrlScorerTest(unittest.TestCase):
  """Tests for the UrlScorer class."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing()
    self.domain1 = 'mydomain.com'
    self.domain2 = 'example.com'
    self.domain3 = 'other.com'
    self.url1 = 'http://mydomain.com/stuff/meep'
    self.url2 = 'http://example.com/some-path?a=b'
    self.url3 = 'http://example.com'
    self.url4 = 'http://other.com/relative'
    self.scorer = dos.UrlScorer(
        period=60,
        min_requests=1,
        max_failure_percentage=0.2,
        prefix='test')

  def testConfig(self):
    """Tests that the config parameters are sanitized."""
    # Bad periods
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=0,
        min_requests=1,
        max_failure_percentage=0.2,
        prefix='test:')
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=-1,
        min_requests=1,
        max_failure_percentage=0.2,
        prefix='test:')
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period='not an int',
        min_requests=1,
        max_failure_percentage=0.2,
        prefix='test:')

    # Bad min_requests
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=1,
        min_requests='bad',
        max_failure_percentage=0.2,
        prefix='test:')
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=1,
        min_requests=-1,
        max_failure_percentage=0.2,
        prefix='test:')

    # Bad max_failure_percentage
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=1,
        min_requests=1,
        max_failure_percentage='not a float',
        prefix='test:')
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=1,
        min_requests=1,
        max_failure_percentage=2,
        prefix='test:')
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=1,
        min_requests=1,
        max_failure_percentage=-1,
        prefix='test:')

    # Bad prefix
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=1,
        min_requests=1,
        max_failure_percentage=0.2,
        prefix='')
    self.assertRaises(dos.ConfigError,
        dos.UrlScorer,
        period=1,
        min_requests=1,
        max_failure_percentage=0.2,
        prefix=123)

  def testReport(self):
    """Tests reporting domain status."""
    self.scorer.report(
        [self.url1, self.url2], [self.url3, self.url4])
    self.assertEquals(1, memcache.get('scoring:test:success:' + self.domain1))
    self.assertEquals(0, memcache.get('scoring:test:failure:' + self.domain1))
    self.assertEquals(1, memcache.get('scoring:test:success:' + self.domain2))
    self.assertEquals(1, memcache.get('scoring:test:failure:' + self.domain2))
    self.assertEquals(0, memcache.get('scoring:test:success:' + self.domain3))
    self.assertEquals(1, memcache.get('scoring:test:failure:' + self.domain3))

    self.scorer.report(
        [self.url1, self.url2, self.url3, self.url4], [])
    self.assertEquals(2, memcache.get('scoring:test:success:' + self.domain1))
    self.assertEquals(0, memcache.get('scoring:test:failure:' + self.domain1))
    self.assertEquals(3, memcache.get('scoring:test:success:' + self.domain2))
    self.assertEquals(1, memcache.get('scoring:test:failure:' + self.domain2))
    self.assertEquals(1, memcache.get('scoring:test:success:' + self.domain3))
    self.assertEquals(1, memcache.get('scoring:test:failure:' + self.domain3))

    self.scorer.report(
        [], [self.url1, self.url2, self.url3, self.url4])
    self.assertEquals(2, memcache.get('scoring:test:success:' + self.domain1))
    self.assertEquals(1, memcache.get('scoring:test:failure:' + self.domain1))
    self.assertEquals(3, memcache.get('scoring:test:success:' + self.domain2))
    self.assertEquals(3, memcache.get('scoring:test:failure:' + self.domain2))
    self.assertEquals(1, memcache.get('scoring:test:success:' + self.domain3))
    self.assertEquals(2, memcache.get('scoring:test:failure:' + self.domain3))

  def testBelowMinRequests(self):
    """Tests when there are enough failures but not enough total requests."""
    memcache.set('scoring:test:success:' + self.domain1, 0)
    memcache.set('scoring:test:failure:' + self.domain1, 10)
    self.assertEquals(
        [(True, 1), (True, 0)],
        self.scorer.filter([self.url1, self.url2]))

  def testFailurePrecentageTooLow(self):
    """Tests when there are enough requests but too few failures."""
    memcache.set('scoring:test:success:' + self.domain1, 100)
    memcache.set('scoring:test:failure:' + self.domain1, 1)
    self.assertEquals(
        [(True, 1/101.0), (True, 0)],
        self.scorer.filter([self.url1, self.url2]))

  def testNotAllowed(self):
    """Tests when a result is blocked due to overage."""
    memcache.set('scoring:test:success:' + self.domain1, 100)
    memcache.set('scoring:test:failure:' + self.domain1, 30)
    self.assertEquals(
        [(False, 30/130.0), (True, 0)],
        self.scorer.filter([self.url1, self.url2]))

  def testGetScores(self):
    """Tests getting the scores of URLs."""
    memcache.set('scoring:test:success:' + self.domain1, 2)
    memcache.set('scoring:test:failure:' + self.domain1, 1)
    memcache.set('scoring:test:success:' + self.domain2, 3)
    memcache.set('scoring:test:failure:' + self.domain2, 3)
    memcache.set('scoring:test:success:' + self.domain3, 1)
    memcache.set('scoring:test:failure:' + self.domain3, 2)
    self.assertEquals(
        [(2, 1), (3, 3), (1, 2)],
        self.scorer.get_scores([self.url1, self.url2, self.url4]))

  def testBlackhole(self):
    """Tests blackholing a URL."""
    self.assertEquals(
      [(True, 0), (True, 0)],
      self.scorer.filter([self.url1, self.url2]))
    self.scorer.blackhole([self.url1, self.url2])
    self.assertEquals(
      [(False, 1.0), (False, 1.0)],
      self.scorer.filter([self.url1, self.url2]))

################################################################################

if __name__ == '__main__':
  unittest.main()
