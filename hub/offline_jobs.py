#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
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

"""Offline cleanup and analysis jobs used with the hub."""

import datetime
import logging
import math
import re
import time

from google.appengine.ext import db

import main

from mapreduce import context
from mapreduce import input_readers
from mapreduce import operation as op
from mapreduce import util
from mapreduce.lib import key_range


def RemoveOldFeedEntryRecordPropertiesMapper(feed_entry_record):
  """Removes old properties from FeedEntryRecord instances."""
  OLD_PROPERTIES = (
      'entry_id_hash',
      'entry_id')
  for name in OLD_PROPERTIES:
    if hasattr(feed_entry_record, name):
      delattr(feed_entry_record, name)
  yield op.db.Put(feed_entry_record)


class CleanupOldEventToDeliver(object):
  """Removes EventToDeliver instances older than a certain value."""

  @staticmethod
  def validate_params(params):
    assert 'age_days' in params
    params['oldest_last_modified'] = (
        time.time() - (86400 * int(params['age_days'])))

  def __init__(self):
    self.oldest_last_modified = None

  def run(self, event):
    if not self.oldest_last_modified:
      params = context.get().mapreduce_spec.mapper.params
      self.oldest_last_modified = datetime.datetime.utcfromtimestamp(
          params['oldest_last_modified'])

    if event.last_modified < self.oldest_last_modified:
      yield op.db.Delete(event)


class CountSubscribers(object):
  """Mapper counts subscribers to a feed pattern by domain.

  Args:
    topic_pattern: Fully-matching regular expression pattern for topics to
      include in the count.
    callback_pattern: Full-matching regular expression pattern for callback
      URLs, where the first group is used as the aggregation key for counters.
  """

  @staticmethod
  def validate_params(params):
    topic_pattern = params['topic_pattern']
    assert topic_pattern and re.compile(topic_pattern)
    callback_pattern = params['callback_pattern']
    assert callback_pattern and re.compile(callback_pattern)

  def __init__(self):
    self.topic_pattern = None
    self.callback_pattern = None

  def run(self, subscription):
    if self.topic_pattern is None:
      params = context.get().mapreduce_spec.mapper.params
      self.topic_pattern = re.compile(params['topic_pattern'])
      self.callback_pattern = re.compile(params['callback_pattern'])

    if self.topic_pattern.match(subscription.topic):
      the_match = self.callback_pattern.match(subscription.callback)
      if the_match:
        yield op.counters.Increment(the_match.group(1))


class HashKeyDatastoreInputReader(input_readers.DatastoreInputReader):
  """A DatastoreInputReader that can split evenly across hash key ranges.

  Assumes key names are in the format supplied by the main.get_hash_key_name
  function.
  """

  @classmethod
  def _split_input_from_namespace(
      cls, app, namespace, entity_kind_name, shard_count):
    entity_kind = util.for_name(entity_kind_name)
    entity_kind_name = entity_kind.kind()

    hex_key_start = db.Key.from_path(
        entity_kind_name, 0)
    hex_key_end = db.Key.from_path(
        entity_kind_name, int('f' * 40, base=16))
    hex_range = key_range.KeyRange(
        hex_key_start, hex_key_end, None, True, True,
        namespace=namespace,
        _app=app)

    key_range_list = [hex_range]
    number_of_half_splits = int(math.floor(math.log(shard_count, 2)))
    for index in xrange(0, number_of_half_splits):
      new_ranges = []
      for current_range in key_range_list:
        new_ranges.extend(current_range.split_range(1))
      key_range_list = new_ranges

    adjusted_range_list = []
    for current_range in key_range_list:
      adjusted_range = key_range.KeyRange(
          key_start=db.Key.from_path(
              current_range.key_start.kind(),
              'hash_%040x' % (current_range.key_start.id() or 0),
              _app=current_range._app),
          key_end=db.Key.from_path(
              current_range.key_end.kind(),
              'hash_%040x' % (current_range.key_end.id() or 0),
              _app=current_range._app),
          direction=current_range.direction,
          include_start=current_range.include_start,
          include_end=current_range.include_end,
          namespace=current_range.namespace,
          _app=current_range._app)

      adjusted_range_list.append(adjusted_range)

    return adjusted_range_list


class SubscriptionReconfirmMapper(object):
  """For reconfirming subscriptions that are nearing expiration."""

  @staticmethod
  def validate_params(params):
    assert 'threshold_timestamp' in params

  def __init__(self):
    self.threshold_timestamp = None

  def run(self, sub):
    if sub.subscription_state != main.Subscription.STATE_VERIFIED:
      return

    if self.threshold_timestamp is None:
      params = context.get().mapreduce_spec.mapper.params
      self.threshold_timestamp = datetime.datetime.utcfromtimestamp(
          float(params['threshold_timestamp']))

    if sub.expiration_time < self.threshold_timestamp:
      sub.request_insert(sub.callback, sub.topic, sub.verify_token,
                         sub.secret, auto_reconfirm=True)
