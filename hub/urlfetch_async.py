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

from google.appengine.api import urlfetch
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import urlfetch_service_pb
from google.appengine.runtime import apiproxy_errors


def fetch(url, payload=None, method=urlfetch.GET, headers={},
          allow_truncated=False, follow_redirects=True,
          callback=None, async_proxy=None,
          deadline=5):
  """Fetches the given HTTP URL, blocking until the result is returned.

  Other optional parameters are:
    method: GET, POST, HEAD, PUT, or DELETE
    payload: POST or PUT payload (implies method is not GET, HEAD, or DELETE)
    headers: dictionary of HTTP headers to send with the request
    allow_truncated: if true, truncate large responses and return them without
      error. otherwise, ResponseTooLargeError will be thrown when a response is
      truncated.
    follow_redirects: Whether or not redirects should be followed.
    callback: Callable that takes (_URLFetchResult, URLFetchException).
      Exactly one of the two arguments is None. Required if async_proxy is
      not None.
    async_proxy: If not None, instance of AsyncAPIProxy to use for executing
      asynchronous API calls.
    deadline: How long to allow the request to wait, in seconds. Defaults
      to 5 seconds.

  We use a HTTP/1.1 compliant proxy to fetch the result.

  The returned data structure has the following fields:
     content: string containing the response from the server
     status_code: HTTP status code returned by the server
     headers: dictionary of headers returned by the server

  If the URL is an empty string or obviously invalid, we throw an
  urlfetch.InvalidURLError. If the server cannot be contacted, we throw a
  urlfetch.DownloadError.  Note that HTTP errors are returned as a part
  of the returned structure, so HTTP errors like 404 do not result in an
  exception.
  """
  request = urlfetch_service_pb.URLFetchRequest()
  response = urlfetch_service_pb.URLFetchResponse()
  request.set_url(url)

  if isinstance(method, basestring):
    method = method.upper()
  method = urlfetch._URL_STRING_MAP.get(method, method)
  if method not in urlfetch._VALID_METHODS:
    raise InvalidMethodError('Invalid method %s.' % str(method))
  if method == urlfetch.GET:
    request.set_method(urlfetch_service_pb.URLFetchRequest.GET)
  elif method == urlfetch.POST:
    request.set_method(urlfetch_service_pb.URLFetchRequest.POST)
  elif method == urlfetch.HEAD:
    request.set_method(urlfetch_service_pb.URLFetchRequest.HEAD)
  elif method == urlfetch.PUT:
    request.set_method(urlfetch_service_pb.URLFetchRequest.PUT)
  elif method == urlfetch.DELETE:
    request.set_method(urlfetch_service_pb.URLFetchRequest.DELETE)

  request.set_followredirects(follow_redirects)

  if payload and (method == urlfetch.POST or method == urlfetch.PUT):
    request.set_payload(payload)

  for key, value in headers.iteritems():
    header_proto = request.add_header()
    header_proto.set_key(key)
    header_proto.set_value(value)

  if async_proxy:
    def completion_callback(response, urlfetch_exception):
      result, user_exception = HandleResult(response, urlfetch_exception,
                                            allow_truncated)
      callback(result, user_exception)
    async_proxy.start_call('urlfetch', 'Fetch', request, response,
                           completion_callback, deadline=deadline)
    return

  user_exception = None
  try:
    apiproxy_stub_map.MakeSyncCall('urlfetch', 'Fetch', request, response)
  except apiproxy_errors.ApplicationError, e:
    user_exception = e

  result, user_exception = HandleResult(
      response, user_exception, allow_truncated)
  if user_exception:
    raise user_exception
  else:
    return result


def HandleResult(response, urlfetch_exception, allow_truncated):
  """Returns (result, user_exception) to return from a fetch() call."""
  result = None
  user_exception = None

  if urlfetch_exception:
    user_exception = urlfetch_exception
    if hasattr(urlfetch_exception, 'application_error'):
      if (urlfetch_exception.application_error ==
          urlfetch_service_pb.URLFetchServiceError.INVALID_URL):
        user_exception = urlfetch.InvalidURLError(str(urlfetch_exception))
      elif (urlfetch_exception.application_error ==
          urlfetch_service_pb.URLFetchServiceError.UNSPECIFIED_ERROR):
        user_exception = urlfetch.DownloadError(str(urlfetch_exception))
      elif (urlfetch_exception.application_error ==
          urlfetch_service_pb.URLFetchServiceError.FETCH_ERROR):
        user_exception = urlfetch.DownloadError(str(urlfetch_exception))
      elif (urlfetch_exception.application_error ==
          urlfetch_service_pb.URLFetchServiceError.RESPONSE_TOO_LARGE):
        user_exception = urlfetch.ResponseTooLargeError(None)
  else:
    result = urlfetch._URLFetchResult(response)
    if not allow_truncated and response.contentwastruncated():
      user_exception = urlfetch.ResponseTooLargeError(result)

  return result, user_exception
