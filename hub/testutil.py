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

"""Utilities common to all tests."""

import StringIO
import logging
import os
import sys
import tempfile
import unittest
import urllib


TEST_APP_ID = 'my-app-id'

# Assign the application ID up front here so we can create db.Key instances
# before doing any other test setup.
os.environ['APPLICATION_ID'] = TEST_APP_ID


def fix_path():
  """Finds the google_appengine directory and fixes Python imports to use it."""
  all_paths = os.environ.get('PATH').split(os.pathsep)
  for path_dir in all_paths:
    dev_appserver_path = os.path.join(path_dir, 'dev_appserver.py')
    if os.path.exists(dev_appserver_path):
      google_appengine = os.path.dirname(os.path.realpath(dev_appserver_path))
      sys.path.append(google_appengine)
      # Use the next import will fix up sys.path even further to bring in
      # any dependent lib directories that the SDK needs.
      dev_appserver = __import__('dev_appserver')
      sys.path.extend(dev_appserver.EXTRA_PATHS)
      return


def setup_for_testing():
  """Sets up the stubs for testing."""
  from google.appengine.api import apiproxy_stub_map
  from google.appengine.api import memcache
  from google.appengine.tools import dev_appserver
  import urlfetch_test_stub
  before_level = logging.getLogger().getEffectiveLevel()
  try:
    logging.getLogger().setLevel(100)
    dev_appserver.SetupStubs(
        TEST_APP_ID,
        login_url='',
        datastore_path=tempfile.mktemp(suffix='datastore_stub'),
        history_path=tempfile.mktemp(suffix='datastore_history'),
        clear_datastore=False)
    apiproxy_stub_map.apiproxy._APIProxyStubMap__stub_map['urlfetch'] = \
        urlfetch_test_stub.instance
    # Actually need to flush, even though we've reallocated. Maybe because the
    # memcache stub's cache is at the module level, not the API stub?
    memcache.flush_all()
  finally:
    logging.getLogger().setLevel(before_level)


def create_test_request(method, *params):
  """Creates a webapp.Request object for use in testing.
  
  Args:
    method: Method to use for the test.
    *params: List of (key, value) tuples to use in the post-body or query
      string of the request.
  
  Returns:
    A new webapp.Request object for testing.
  """
  from google.appengine.ext import webapp
  body = StringIO.StringIO()
  encoded_params = urllib.urlencode(params)
  environ = os.environ.copy()
  environ.update({
    'QUERY_STRING': '',
    'wsgi.input': body,
  })
  if method.lower() == 'get':
    environ['REQUEST_METHOD'] = method.upper()
    environ['QUERY_STRING'] = encoded_params
  else:
    body.write(encoded_params)
    body.seek(0)
    environ['REQUEST_METHOD'] = method.upper()
    environ['CONTENT_TYPE'] = 'application/x-www-form-urlencoded'
    environ['CONTENT_LENGTH'] = str(len(body.getvalue()))
  return webapp.Request(environ)


class HandlerTestBase(unittest.TestCase):
  """Base-class for webapp.RequestHandler tests."""
  
  # Set to the class being tested.
  handler_class = None

  def setUp(self):
    """Sets up the test harness."""
    setup_for_testing()

  def tearDown(self):
    """Tears down the test harness."""
    pass

  def handle(self, method, *params):
    """Runs a test of a webapp.RequestHandler.
    
    Args:
      method: The method to invoke for this test.
      *params: Passed to testutil.create_test_request
    """
    from google.appengine.ext import webapp
    before_software = os.environ.get('SERVER_SOFTWARE')
    before_auth_domain = os.environ.get('AUTH_DOMAIN')
    before_email = os.environ.get('USER_EMAIL')

    os.environ['wsgi.url_scheme'] = 'http'
    os.environ['SERVER_NAME'] = 'example.com'
    os.environ['SERVER_PORT'] = ''
    try:
      if not before_software:
        os.environ['SERVER_SOFTWARE'] = 'Development/1.0'
      if not before_auth_domain:
        os.environ['AUTH_DOMAIN'] = 'example.com'
      if not before_email:
        os.environ['USER_EMAIL'] = ''
      self.resp = webapp.Response()
      self.req = create_test_request(method, *params)
      handler = self.handler_class()
      handler.initialize(self.req, self.resp)
      getattr(handler, method.lower())()
      logging.info('%r returned status %d: %s', self.handler_class,
                   self.response_code(), self.response_body())
    finally:
      del os.environ['SERVER_SOFTWARE']
      del os.environ['AUTH_DOMAIN']
      del os.environ['USER_EMAIL']

  def response_body(self):
    """Returns the response body after the request is handled."""
    return self.resp.out.getvalue() 

  def response_code(self):
    """Returns the response code after the request is handled."""
    return self.resp._Response__status[0]  
