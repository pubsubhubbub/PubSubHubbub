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

"""Decorators and utilities for attack protection and statistics gathering."""

import gc
import logging
import os
import random
import re
import struct
import time

from google.appengine.api import memcache


# Set to true in tests to disable DoS protection.
DISABLE_FOR_TESTING = False


class ConfigError(Exception):
  """Something is wrong with a configured DoS limit, sampler, or scorer."""

################################################################################


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

      if DISABLE_FOR_TESTING:
        return func(myself, *args, **kwargs)

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
        if (result - count) == 1:
          log_level = logging.error
        else:
          log_level = logging.debug
        log_level('Hit rate limit on "%s" by "%s" where '
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

    decorated.func_name = func.func_name  # Fun with hacking the Python stack!
    return decorated

  return wrapper

################################################################################

# TODO: Determine if URL/domain caching is necessary due to regex performance.

# TODO: Add ane exception list of domains that should use the full domain,
# not just the last suffix, when going through the get_url_domain. This is
# needed for domains like 'appspot.com' that are shared across totally
# different developers.

# Matches four groups:
#   1) an IP, 2) a domain prefix, 3) a domain suffix, 4) other (eg, localhost)
URL_DOMAIN_RE = re.compile(
    r'https?://(?:'
    r'([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)|'  # IP address
    r'(?:((?:[a-zA-Z0-9-]+\.)*)([a-zA-Z0-9-]+\.[a-zA-Z0-9-]+))|'  # Domain
    r'([^/]+)'  # Anyting else
    r')(?:/.*)?')  # The rest of the URL

# Domains where only the suffix is important.
DOMAIN_SUFFIX_EXCEPTIONS = frozenset([
  'blogspot.com',
  'livejournal.com',
])

# Maximum size of the cache of URLs to domains.
DOMAIN_CACHE_SIZE = 100

# Simple local cache used for per-request URL to domain mappings.
_DOMAIN_CACHE = {}


def get_url_domain(url):
  """Returns the domain for a URL or 'bad_url if it's not a valid URL."""
  result = _DOMAIN_CACHE.get(url)
  if result is not None:
    return result
  if len(_DOMAIN_CACHE) >= DOMAIN_CACHE_SIZE:
    _DOMAIN_CACHE.clear()

  match = URL_DOMAIN_RE.match(url)
  if match:
    groups = list(match.groups())
    if groups[1] and groups[2] and groups[2] not in DOMAIN_SUFFIX_EXCEPTIONS:
      groups[2] = groups[1] + groups[2]
    groups[1] = None
    groups = filter(bool, groups)
  else:
    groups = []
  result = (groups + ['bad_url'])[0]

  _DOMAIN_CACHE[url] = result
  return result

################################################################################

def offset_or_add(offsets,
                  period,
                  prefix='',
                  offset_multi=memcache.offset_multi,
                  add_multi=memcache.add_multi):
  """Offsets values in memcache or re-adds them if not present.

  This method is required when you want to offset memcache values *and* set
  their expiration time to a distinct time in the future.

  Args:
    offsets: Dictionary mapping keys to offset integers.
    period: Time in seconds before these keys should expire.
    prefix: Any memcache prefix to use for these keys.
    offset_multi: Used for testing.
    add_multi: Used for testing.

  Returns:
    Dictionary mapping input keys to updated values. None will be returned
    for keys that could not be updated.
  """
  results = offset_multi(offsets, key_prefix=prefix)

  # Add any non-existent items.
  adds = {}
  for key, value in results.iteritems():
    if value is None:
      adds[key] = offsets[key]
  if adds:
    failed = set(add_multi(adds, time=period, key_prefix=prefix))
    for key, value in adds.iteritems():
      if key not in failed:
        results[key] = value
  else:
    failed = set()

  # If the add fails, then someone else won the race, so increment.
  second_offsets = dict((k, offsets[k]) for k in failed)
  if second_offsets:
    second_results = offset_multi(second_offsets, key_prefix=prefix)
  else:
    second_results = {}

  failed_keys = set(k for k, v in second_results.iteritems() if v is None)
  if failed_keys:
    logging.critical('Failed memcache offset_or_add for prefix=%r, keys=%r',
                     prefix, failed_keys)

  results.update(second_results)
  return results

################################################################################

# TODO: Support more than 1MB of sample data by using multiple memcache calls
# to retrieve all of the sample values.

# TODO: Support configs that *always* sample particular URLs or domains.
# Could have a console insert a list of values into memcache which is fetched
# and cached in each runtime, and then those domains would always match.

# TODO: Allow for synchronized reservoirs that reset at synchronized intervals.
# This will let us view overlapping windows of samples across a time period
# instead of having to reset every N seconds.

class ReservoirConfig(object):
  """Configuration for a reservoir sampler."""

  def __init__(self,
               name,
               period=None,
               samples=None,
               by_domain=False,
               by_url=False,
               rate=1,
               max_value_length=75,
               tolerance=10,
               title=None,
               key_name='Key',
               value_units=''):
    """Initializer.

    Args:
      name: Programmatic name to use for this reservoir in memcache.
      period: Time period for this reservoir in seconds.
      samples: Total number of samples to use in the reservoir.
      by_domain: True if URL domains should be used as the sampling key.
      by_url: True if the whole URL should be used as the sampling key.
      rate: Sampling rate (between 0 and 1) to reduce the latency overhead of
        applying this sampler.
      max_value_length: Length to truncate a sampling key to before storing
        it in memcache.
      tolerance: Number of seconds to allow for samples to stay valid for
        after a reservoir reset has happened.
      title: Nice-looking title of this config; will use the name if
        not supplied.
      key_name: The noun of the key (e.g., 'domain' or 'url').
      value_units: The noun of the value units (e.g., 'milliseconds').
    """
    if not name:
      raise ConfigError('Must specify a name')

    try:
      period = int(period)
    except ValueError, e:
      raise ConfigError('Invalid period: %s' % e)
    if period <= 0:
      raise ConfigError('period must be positive')

    try:
      samples = int(samples)
    except ValueError, e:
      raise ConfigError('Invalid samples: %s' % e)
    if samples <= 0:
      raise ConfigError('samples must be positive')

    if not (by_domain ^ by_url):
      raise ConfigError('Must specify by_domain or by_url')

    try:
      rate = float(rate)
    except ValueError, e:
      raise ConfigError('Invalid rate: %s' % e)
    if not (0 <= rate <= 1):
      raise ConfigError('rate must be between 0 and 1')

    try:
      tolerance = int(tolerance)
    except ValueError, e:
      raise ConfigError('Invalid tolerance: %s' % e)
    if tolerance < 0:
      raise ConfigError('tolerance must be non-negative')

    self.name = name
    self.title = title or name
    self.key_name = key_name
    self.value_units = value_units
    self.samples = samples
    self.rate = rate
    self.period = period
    self.inverse_rate = 1.0 / self.rate
    self.by_url = by_url
    self.by_domain = by_domain
    self.max_value_length = max_value_length
    self.tolerance = tolerance

    if by_url:
      self.kind = 'by_url'
    else:
      self.kind = 'by_domain'
    self.counter_key = '%s:%s:counter' % (self.name, self.kind)
    self.start_key = '%s:%s:start_time' % (self.name, self.kind)
    self._position_key_template = '%s:%s:%%d' % (self.name, self.kind)

  def position_key(self, index):
    """Generates the position key for the sample slot with the given index.

    Args:
      index: Numerical index of the sample position who's key to retrieve.

    Returns:
      Memcache key to use.
    """
    return self._position_key_template % index

  def is_expired(self, last_time, current_time):
    """Checks if this config is expired.

    Args:
      last_time: UNIX timestamp when this config's period started.
      current_time: UNIX timestamp of the current time.

    Returns:
      True if the config period has expired.
    """
    return (current_time - last_time) > self.period

  def adjust_value(self, key):
    """Adjust the value for a sampling key.

    Args:
      key: The sampling key to adjust.

    Returns:
      The adjusted key.
    """
    if self.by_url:
      value = key
    else:
      value = get_url_domain(key)
    if len(value) > self.max_value_length:
      value = value[:self.max_value_length]
    if isinstance(value, unicode):
      value = unicode(value).encode('utf-8')
    return value

  def should_sample(self, key, coin_flip):
    """Checks if the key should be sampled.

    Args:
      key: The sampling key to check.
      coin_flip: Random value between 0 and 1.

    Return:
      True if the sample should be taken, False otherwise.
    """
    return coin_flip < self.rate

  def compute_frequency(self, count, found, total, elapsed):
    """Computes the frequency of a sample.

    Args:
      count: Total number of samples of this key.
      found: Total number of samples present for all keys.
      total: The total number of sampling events so far, regardless of
        whether or not the sample was saved.
      elapsed: Seconds elapsed during the current sampling period.

    Returns:
      The frequency, in events per second, of this key in the time period,
      or None if no samples have been taken yet.
    """
    if not total or not found:
      return None
    return self.inverse_rate * (1.0 * count / found) * (1.0 * total / elapsed)


class Reporter(object):
  """Contains a batch of keys and values for potential sampling."""

  def __init__(self):
    """Initializer."""
    # Keep a list of input keys in order so we can iterate through the
    # dictionary in order during testing. This costs little and vastly
    # simplifies testing.
    self.keys = []
    # Maps key -> {config -> value}
    self.param_dict = {}
    # Maps config -> [key, ...]
    self.config_dict = {}

  def set(self, key, config, value=1):
    """Sets a key/value for a specific config.

    Each config/key combination may only have a single value. Subsequent
    calls to this method with the same key/config will overwrite the
    previous value.

    Args:
      key: The sampling key to add.
      config: The ReservoirConfig object to set the value for.
      value: The value to set for this config.
    """
    value_dict = self.param_dict.get(key)
    if value_dict is None:
      self.param_dict[key] = value_dict = {}
    value_dict[config] = value
    self.keys.append(key)

    present_list = self.config_dict.get(config)
    if present_list is None:
      self.config_dict[config] = present_list = []
    present_list.append(key)

  def get(self, key, config):
    """Gets the value for a key/config.

    Args:
      key: The sampling key to retrieve the value for.
      config: The ReservoirConfig object to get the value for.

    Returns:
      The value for the key/config or None if it's not present.
    """
    return self.param_dict.get(key, {}).get(config)

  def remove(self, key, config):
    """Removes a key/value for a specific config.

    If the key is not present for the config, this method does nothing.

    Args:
      key: The sampling key to remove.
      config: The ReservoirConfig object to remove the key for.
    """
    try:
      del self.param_dict[key][config]
      self.config_dict[config].remove(key)
    except KeyError:
      pass

  def all_keys(self):
    """Returns all the sampling keys present across all configs.

    Each key will be present at least once, but some keys may be present
    more than once if they were inserted repeatedly. The keys are in
    insertion order. This simplifies testing of this class.
    """
    return self.keys

  def get_keys(self, config):
    """Retrieves the keys present for a specific ReservoirConfig.

    Args:
      config: The ReservoirConfig object to get the keys for.

    Returns:
      The list of keys present for this config, with no duplicates.
    """
    return self.config_dict.get(config, [])


class SampleResult(object):
  """Contains the current results of a sampler for a given config."""

  def __init__(self, config, total_samples, time_elapsed):
    """Initializer.

    Args:
      config: The ReservoirConfig these results are for.
      total_samples: The total number of sampling events that have occurred.
        This is *not* the number of unique samples present in the table.
      time_elapsed: Time in seconds that have elapsed in the current period.
    """
    self.config = config
    self.total_samples = total_samples
    self.time_elapsed = time_elapsed
    self.unique_samples = 0
    self.title = config.title
    self.key_name = config.key_name
    self.value_units = config.value_units

    # Maps key -> [(when, value), ...]
    self.sample_dict = {}

  def add(self, key, when, value):
    """Adds a new sample to these results.

    Args:
      key: The sampling key.
      when: When the sample was made, as a UNIX timestamp.
      value: The value that was sampled.
    """
    samples = self.sample_dict.get(key)
    if samples is None:
      self.sample_dict[key] = samples = []
    samples.append((when, value))
    self.unique_samples += 1

  def overall_rate(self):
    """Gets the overall rate of events.

    Returns:
      Total events per second.
    """
    return 1.0 * self.total_samples / self.time_elapsed

  def get_min(self, key):
    """Gets the min value seen for a key.

    Args:
      key: The sampling key.

    Returns:
      The minimum value or None if this key does not exist.
    """
    samples = self.sample_dict.get(key)
    if samples is None:
      return None
    return min(samples, key=lambda x: x[1])[1]

  def get_max(self, key):
    """Gets the max value seen for a key.

    Args:
      key: The sampling key.

    Returns:
      The maximum value or None if this key does not exist.
    """
    samples = self.sample_dict.get(key)
    if samples is None:
      return None
    return max(samples, key=lambda x: x[1])[1]

  def get_frequency(self, key):
    """Gets the frequency of events for this key during the sampling period.

    Args:
      key: The sampling key.

    Returns:
      The frequency as events per second or None if this key does not exist.
    """
    samples = self.sample_dict.get(key)
    if samples is None:
      return None
    return self.config.compute_frequency(
        len(samples),
        self.unique_samples,
        self.total_samples,
        self.time_elapsed)

  def get_average(self, key):
    """Gets the weighted average of this key's sampled values.

    Args:
      key: The sampling key.

    Returns:
      The weighted average or None if this key does not exist.
    """
    samples = self.sample_dict.get(key)
    if not samples:
      return None
    total = 0.0
    for sample in samples:
      total += sample[1]
    return total / len(samples)

  def get_count(self, key):
    """Gets the count of unique samples for a key.

    Args:
      key: The sampling key.

    Returns:
      The number of items. Will be zero if the key does not exist.
    """
    return len(self.sample_dict.get(key, []))

  def get_samples(self, key):
    """Gets the unique sample data for a key.

    Args:
      key: The sampling key.

    Returns:
      List of tuple (when, value) where:
        when: The UNIX timestamp for the sample.
        value: The sample value.
    """
    return self.sample_dict.get(key, [])

  def set_single_sample(self, key):
    """Sets that this result is for a single key.

    Args:
      key: The sampling key.
    """
    self.total_samples = self.get_count(key)

  def sample_objects(self):
    """Gets the contents of this result object for use in template rendering.

    Returns:
      Generator of model objects.
    """
    for key in self.sample_dict:
      yield {
        'key': key,
        'count': self.get_count(key),
        'frequency': self.get_frequency(key),
        'min': self.get_min(key),
        'max': self.get_max(key),
        'average': self.get_average(key),
      }


class MultiSampler(object):
  """Sampler that saves key/value pairs for multiple reservoirs in parallel.

  The basic algorithm is:

    1. Get the reservoir start timestamp.
    2. If more than period seconds have elapsed, set the timestamp to now, set
       the reservoir's event counter to zero (average case this is skipped).
    3. Increment the event counter by the number of new samples.
    4. Set memcache values to incoming samples following the reservoir
       algorithm, potentially only sampling a subset.

  The benefit of this approach is it can be applied to many reservoirs in
  parallel without incurring additional API calls. The only limit is the 1MB
  limit on App Engine API calls, which puts a cap on the amount of samples
  that can be made simultaneously.

  Samples are stored in keys like: 'sampler_name:0', 'sampler_name:1'

  Values stored for samples look like: 'key_sample:NNNN:WWWW' where the 'N's
  represent the sample value as a big-endian-encoded 4-byte string, and the
  'W's are a UNIX timestamp as a big-endian-encoded 4-byte string. The
  timestamp is used to ignore samples that are not from the current period.

  There can be a race for resetting the timestamp for a sampler right after
  the period starts, but it always favors the caller who inserted last (all
  earlier data will be overwritten). This results in some missing data for
  short-period samplers, but it's okay.
  """

  def __init__(self, configs, gettime=time.time):
    """Initializer.

    Args:
      configs: Iterable of ReservoirConfig objects.
      gettime: Used for testing.
    """
    self.configs = list(configs)
    self.gettime = gettime

  def sample(self,
             reporter,
             getrandom=random.random,
             randrange=random.randrange):
    """Samples a set of reported key/values.

    Args:
      reporter: Reporter instance containing key/values to sample.
      getrandom: Used for testing.
      randrange: Used for testing.
    """
    # Update period start times if they're expired or non-existent.
    now = int(self.gettime())
    start_times = memcache.get_multi([c.start_key for c in self.configs])
    config_sets = {}
    for config in self.configs:
      start = start_times.get(config.start_key)
      if start is None or config.is_expired(start, now):
        config_sets[config.start_key] = now
        config_sets[config.counter_key] = 0
    if config_sets:
      memcache.set_multi(config_sets)

    # Flip coin for sample rate of all Keys on all configs.
    for key in reporter.all_keys():
      coin_flip = getrandom()
      for config in self.configs:
        if not config.should_sample(key, coin_flip):
          reporter.remove(key, config)

    # Increment counters for affected configs.
    counter_offsets = {}
    for config in self.configs:
      matching = reporter.get_keys(config)
      if matching:
        counter_offsets[config.counter_key] = len(matching)
    if not counter_offsets:
      return
    counter_results = memcache.offset_multi(counter_offsets, initial_value=0)

    # Apply the reservoir algorithm.
    value_sets = {}
    now_encoded = struct.pack('!l', now)
    for config in self.configs:
      matching = list(reporter.get_keys(config))
      counter = counter_results.get(config.counter_key)
      if counter is None:
        # Incrementing the config failed, so give up on these Key samples.
        continue
      counter = int(counter)  # Deal with wonky serialization types.
      for (value_index, sample_number) in zip(
          xrange(len(matching)), xrange(counter - len(matching), counter)):
        insert_index = None
        if sample_number < config.samples:
          insert_index = sample_number
        else:
          random_index = randrange(sample_number)
          if random_index < config.samples:
            insert_index = random_index
        if insert_index is not None:
          key = matching[value_index]
          value_key = config.position_key(insert_index)
          value = reporter.get(key, config)
          if value is not None:
            # Value may be none if this key was removed from the samples
            # list due to not passing the coin flip.
            value_encoded = struct.pack('!l', value)
            sample = '%s:%s:%s' % (
                config.adjust_value(key), now_encoded, value_encoded)
            value_sets[value_key] = sample
    memcache.set_multi(value_sets)

  def get(self, config, single_key=None):
    """Gets statistics for a particular config and/or key.

    This will only retrieve samples for the current time period. Samples
    from previous time periods will be ignored.

    Args:
      config: The ReservoirConfig to retrieve stats for.
      single_key: If None, then global stats for the config will be retrieved.
        When a key value (a string), then only stats for that particular key
        will be returned to the caller.

    Returns:
      SampleResult object containing the result data.
    """
    # Make sure the key is converted into the format expected by the config.
    if single_key is not None:
      single_key = config.adjust_value(single_key)

    keys = [config.start_key, config.counter_key]
    for i in xrange(config.samples):
      keys.append(config.position_key(i))
    sample_data = memcache.get_multi(keys)

    # Deal with wonky serialization types.
    counter = int(sample_data.get(config.counter_key, 0))
    start_time = sample_data.get(config.start_key)
    now = self.gettime()
    if start_time is None:
      # If the start time isn't there, then just assume it started exactly
      # the period ago. This should only happen if the start time gets
      # evicted for some weird reason.
      start_time = now - config.period
    elapsed = now - start_time

    # Find all samples that fall within the reset time validity window.
    results = SampleResult(config, counter, elapsed)
    for i in xrange(config.samples):
      combined_value = sample_data.get(config.position_key(i))
      if combined_value is None:
        continue
      key, when_encoded, value_encoded = (
          combined_value.rsplit(':', 2) + ['', '', ''])[:3]
      if single_key is not None and single_key != key:
        continue

      if len(when_encoded) != 4:
        continue
      when = struct.unpack('!l', when_encoded)[0]
      if len(value_encoded) != 4:
        continue
      value = struct.unpack('!l', value_encoded)[0]

      if ((start_time - config.tolerance)
          < when <
          (start_time + config.period + config.tolerance)):
        results.add(key, when, value)

    # For a single sample we need to set the counter to the number of unique
    # samples so we don't leak the overall QPS being pushed for this event.
    if single_key is not None:
      results.set_single_sample(single_key)

    return results

  def get_chain(self, *configs, **kwargs):
    """Gets statistics for a set of configs, optionally for a single key.

    For retrieving multiple configs sequentially in a way that ensures that
    the memory usage of the previous result is garbage collected before the
    next one is returned.

    Args:
      *configs: Set of configs to retrieve.
      **kwargs: Keyword arguments to pass to the 'get' method of this class.

    Returns:
      Generator that yields each SampleResult object for each config.
    """
    for config in configs:
      result = self.get(config, **kwargs)
      yield result
      del result
      # NOTE: This kinda sucks, but the result sets are really large so
      # we need to make sure the garbage collector is doing its job so we
      # don't run bloat memory over the course of a single stats request.
      gc.collect()

################################################################################

class UrlScorer(object):
  """Classifies incoming URLs by domain as passing a filter or being blocked.

  Used to enforce per-callback and per-feed limits on slowness, failures,
  and misbehavior.
  """

  def __init__(self,
               period=None,
               min_requests=None,
               max_failure_percentage=None,
               prefix=None):
    """Initializer.

    Args:
      period: Over what time period, in seconds, the scorer should track
        statistics for URLs. The shorter the more reliable the enforcement
        due to cache eviction. Must be a positive number, forced to integer.
      min_requests: Minimum number of requests to receive (per second,
        independent of the enforcement 'period' parameter) before rate limiting
        takes effect. Must be non-negative. Floating point is fine.
      max_failure_percentage: Maximum percentage of failures (as a moving
        rate) to allow before a domain is blocked. Value can be a number
        between 0 and 1 and float.
      prefix: Memcache key prefix to use for this scorer configuration.
        Must not be empty.

    Raises:
      ConfigError if any of the parameters above are invalid.
    """
    try:
      period = int(period)
    except ValueError, e:
      raise ConfigError('Invalid period: %s' % e)
    if period <= 0:
      raise ConfigError('period must be non-zero')

    try:
      min_requests = float(min_requests)
    except ValueError, e:
      raise ConfigError('Invalid min_requests: %s' % e)
    if min_requests < 0:
      raise ConfigError('min_requests must be non-negative')

    try:
      max_failure_percentage = float(max_failure_percentage)
    except ValueError, e:
      raise ConfigError('invalid max_failure_percentage: %s' % e)
    if not (0 <= max_failure_percentage <= 1):
      raise ConfigError('max_failure_percentage must be between 0 and 1')

    if not isinstance(prefix, basestring) or not prefix:
      raise ConfigError('prefix must be a non-empty string')

    self.period = period
    self.min_requests = int(self.period * min_requests)
    self.max_failure_percentage = max_failure_percentage
    self.prefix = 'scoring:%s:' % prefix

  def filter(self, urls):
    """Checks if each URL can proceed based on a successful score.

    Args:
      urls: Iterable of URLs to check. Each input URL will have a corresponding
        result returned in the same order they were passed in.

    Returns:
      List of tuple (allowed, failure_percentage) where:
        allowed: True if the URL passed the filter, False otherwise.
        failure_percentage: Percent of failures seen during the current
          scoring period. Number between 0 and 1.
    """
    domain_list = [get_url_domain(u) for u in urls]
    keys = ['success:' + d for d in domain_list]
    keys.extend('failure:' + d for d in domain_list)
    values = memcache.get_multi(keys, key_prefix=self.prefix)

    result = []
    for domain in domain_list:
      success = values.get('success:' + domain, 0)
      failure = values.get('failure:' + domain, 0)
      requests = success + failure

      if requests > 0:
        failure_percentage = (1.0 * failure) / requests
      else:
        failure_percentage = 0

      allow = bool(
          DISABLE_FOR_TESTING or
          requests < self.min_requests or
          failure_percentage < self.max_failure_percentage)
      result.append((allow, failure_percentage))

    return result

  def report(self, success, failure):
    """Reports the status of interactions with a set of URLs.

    Args:
      success: Iterable of URLs that had successful interactions.
      failure: Iterable of URLs that had failed interactions.
    """
    if success is None:
      success = []
    if failure is None:
      failure = []

    offsets = {}
    # Always set successful and failed even if the other one doesn't exist yet
    # so we're reasonably sure that their expiration times are synchronized.
    for url in success:
      domain = get_url_domain(url)
      success_key = 'success:' + domain
      fail_key = 'failure:' + domain
      offsets[success_key] = offsets.get(success_key, 0) + 1
      offsets[fail_key] = offsets.get(fail_key, 0)
    for url in failure:
      domain = get_url_domain(url)
      success_key = 'success:' + domain
      fail_key = 'failure:' + domain
      offsets[success_key] = offsets.get(success_key, 0)
      offsets[fail_key] = offsets.get(fail_key, 0) + 1

    offset_or_add(offsets, self.period, self.prefix)

  def get_scores(self, urls):
    """Retrieves the scores for a set of URLs.

    Args:
      urls: Iterable of URLs to retrieve the scores for. Each input will have
        a corresponding entry in the returned value in the same order.

    Returns:
      List of tuple (success, failure) where:
        success: Number of successful requests.
        failure: Number of failed requests.
    """
    domain_list = [get_url_domain(u) for u in urls]
    keys = ['success:' + d for d in domain_list]
    keys.extend('failure:' + d for d in domain_list)
    values = memcache.get_multi(keys, key_prefix=self.prefix)
    return [(values.get('success:' + d, 0), values.get('failure:' + d, 0))
            for d in domain_list]

  def blackhole(self, urls):
    """Blackholes a set of URLs by domain for the rest of the current period.

    Args:
      urls: Iterable of URLs to blackhole.
    """
    values = dict(('failure:' + get_url_domain(u), self.min_requests)
                  for u in urls)
    memcache.set_multi(values, key_prefix=self.prefix)
