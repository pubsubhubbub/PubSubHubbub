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

"""URLFetchServiceStub implementation that returns mock values."""

import logging

from google.appengine import runtime
from google.appengine.api import apiproxy_stub
from google.appengine.api import urlfetch_service_pb
from google.appengine.api import urlfetch_stub
from google.appengine.runtime import apiproxy_errors


class URLFetchServiceTestStub(urlfetch_stub.URLFetchServiceStub):
  """Enables tests to mock calls to the URLFetch service and test inputs."""
  
  def __init__(self):
    """Initializer."""
    super(URLFetchServiceTestStub, self).__init__()
    # Maps (method, url) keys to (request_payload, response_code,
    # response_data, error_instance)
    self._expectations = {}
  
  def clear(self):
    """Clears all expectations on this stub."""
    self._expectations.clear()
  
  def expect(self, method, url, response_code, response_data,
             request_payload='', urlfetch_error=False, apiproxy_error=False,
             deadline_error=False):
    """Expects a certain request and response.
    
    Overrides any existing expectations for this stub.
    
    Args:
      method: The expected method.
      url: The expected URL to access.
      response_code: The expected response code.
      response_data: The expected response data.
      request_payload: The expected request payload, if any.
      urlfetch_error: Set to True if this call should raise a
        urlfetch_errors.Error exception when made.
      apiproxy_error: Set to True if this call should raise an
        apiproxy_errors.Error exception when made.
      deadline_error: Set to True if this call should raise a
        google.appengine.runtime.DeadlineExceededError error.
    """
    error_instance = None
    if urlfetch_error:
      error_instance = apiproxy_errors.ApplicationError(
          urlfetch_service_pb.URLFetchServiceError.FETCH_ERROR, 'mock error')
    elif apiproxy_error:
      error_instance = apiproxy_errors.OverQuotaError()
    elif deadline_error:
      error_instance = runtime.DeadlineExceededError()

    self._expectations[(method.lower(), url)] = (
        request_payload, response_code, response_data, error_instance)
  
  def verify_and_reset(self):
    """Verify that all expectations have been met and clear any remaining."""
    old_expectations = self._expectations
    self._expectations = {}
    if old_expectations:
      assert False, '%d expectations remain: %r' % (
          len(old_expectations), old_expectations)

  def _RetrieveURL(self, url, payload, method, headers, response,
                   follow_redirects=True):
    """Test implementation of retrieving a URL.

    Args:
      All override super-class's parameters.
    """
    header_text = None
    if headers:
      header_text = ', '.join('%s=%s' % (h.key(), h.value()) for h in headers)
    logging.info('Received URLFetch request:\n%s %s\nHeaders: %r\nPayload: %r',
        method, url,
        header_text, payload)

    key = (method.lower(), url)
    try:
      expected = self._expectations.pop(key)
    except:
      assert False, 'Did not expect: %s %s' % key

    if expected[0]:
      assert payload == expected[0], (
        'Request payload: "%s" did not match expected: "%s"' %
        (expected[0], payload))
    if expected[3] is not None:
      raise expected[3]

    response.set_statuscode(expected[1])
    response.set_content(expected[2])


instance = URLFetchServiceTestStub()
