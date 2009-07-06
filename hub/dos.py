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

"""Decorators for denial-of-service attack protection and rate-limiting."""

import logging
import os

from google.appengine.api import memcache


# TODO:
#
# - Add statistical sampling to provide runtime stats of current top-N
#   accessors by limit key.
#
# - Add web console for viewing runtime stats, clearing them.
#

class ConfigError(Exception):
  """Something is wrong with a configured DoS limit."""


_DEFAULT_MESSAGE = (
    'Too many requests for "%(key)s"; '
    'current rate is %(rate).3f/s, '
    'limit is %(limit).3f/s')


def limit(param=None,
          header='REMOTE_ADDR',
          count=None,
          period=None,
          error_code=503,
          retry_after=120,
          message=_DEFAULT_MESSAGE,
          param_whitelist=None,
          header_whitelist=None):
  """Limits a webapp.RequestHandler method to a specific rate.

  Either 'param', 'header', or both 'param' and 'header' must be specified. If
  values cannot be found for either of these, the request will be allowed to
  go through. Unlike the limiting constraints, if whitelists are supplied, then
  *any* match of either whitelist will cause the dos limit to be skipped.

  Args:
    func: The RequestHandler method to decorate with the rate limit.
    param: A request parameter to use for rate limiting. If None, no parameters
      will be used.
    header: Header to use for rate limiting. If None, no header value will be
      used. This header name must be in the CGI environment variable format,
      e.g., HTTP_X_FORWARDED_FOR.
    count: Maximum number of executions of this function to allow.
    period: Period over which the 'count' executions should be allowed,
      specified in seconds. Must be less than a month in length.
    error_code: Error code to return when the rate limit has been exceeded.
      Defaults to 503.
    retry_after: Number of seconds to return for the 'Retry-After' header when
      an error is served. If None, no header will be returned.
    message: Error message to serve in the body of an error response. May have
      formatting parameters 'key', 'rate', and 'limit'.
    param_whitelist: If not None, a set of values of 'param' that are allowed
      to pass the dos limit without throttling.
    header_whitelist: If not None, a set of values of 'header' that are allowed
      to pass the dos limit without throttling.

  Returns:
    The decorated method.

  Raises:
    ConfigError at decoration time if any rate limit parameters are invalid.
  """
  if not (param or header):
    raise ConfigError('Must specify "param" and/or "header" keywords')
  if count is None or count < 0:
    raise ConfigError('Must specify count >= 0')
  if period is None or period < 1:
    raise ConfigError('Must specify period >= 1')

  limit = float(count) / period
  required_parts = 2  # two becuase path and method name are always in the key
  if param:
    required_parts += 1
  if header:
    required_parts += 1
  if param_whitelist is None:
    param_whitelist = frozenset([])
  if header_whitelist is None:
    header_whitelist = frozenset([])

  def wrapper(func):
    if func.func_name not in ('post', 'get') and param:
      raise ConfigError('May only specify param limit for GET and POST')
    def decorated(myself, *args, **kwargs):
      method = myself.request.method
      parts = [method, myself.request.path]
      whitelisted = False

      if param:
        value = myself.request.get(param)
        if value:
          parts.append('%s=%s' % (param, value))
          if value in param_whitelist:
            whitelisted = True
      if header:
        value = os.environ.get(header)
        if value:
          parts.append('%s=%s' % (header, value))
          if value in header_whitelist:
            whitelisted = True

      key = ' '.join(parts)
      result = None
      if len(parts) != required_parts:
        logging.critical('Incomplete rate-limit key = "%s" for param = "%s", '
                         'header = "%s" on "%s" where count = %s, period = %s, '
                         'limit = %.3f/sec', key, param, header, method,
                         count, period, limit)
      else:
        result = memcache.incr(key)
        if result is None:
          # Rate limit not yet in memcache.
          result = 1
          if not memcache.add(key, result, time=period):
            # Possible race for who adds to the cache first.
            result = memcache.incr(key)
            if result is None:
              # Memcache definitely down.
              skip_enforcement = True
              logging.error('Memcache failed for rate limit on "%s" by "%s" '
                            'where count = %s, period = %s, limit = %.3f/s',
                            method, key, count, period, limit)

      if not whitelisted and result > count:
        rate = float(result) / period
        logging.error('Hit rate limit on "%s" by "%s" where '
                      'count = %s, period = %s, rate = %.3f/s, limit = %.3f/s',
                      method, key, count, period, rate, limit)
        myself.response.set_status(error_code)
        myself.response.headers['Content-Type'] = 'text/plain'
        if retry_after is not None:
          myself.response.headers['Retry-After'] = str(retry_after)
        values = {'key': key, 'rate': rate, 'limit': limit}
        myself.response.out.write(message % values)
      else:
        return func(myself, *args, **kwargs)

    return decorated

  return wrapper
