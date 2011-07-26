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
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import util


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
  """Mapper counts active subscribers to a feed pattern by domain.

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
      if (the_match and
          subscription.subscription_state == main.Subscription.STATE_VERIFIED):
        yield op.counters.Increment(the_match.group(1))
      elif the_match:
        yield op.counters.Increment('matched but inactive')


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


def count_subscriptions_for_topic(subscription):
  """Counts a Subscription instance if it's still active."""
  print subscription.subscription_state
  if subscription.subscription_state == main.Subscription.STATE_VERIFIED:
    yield (subscription.topic_hash, '1')


def save_subscription_counts_for_topic(topic_hash, counts):
  """Sums subscriptions to a topic and saves a corresponding KnownFeedStat."""
  total_count = len(counts)
  entity = main.KnownFeedStats(
      key=main.KnownFeedStats.create_key(topic_hash=topic_hash),
      subscriber_count=total_count)
  yield op.db.Put(entity)


def start_count_subscriptions():
  """Kicks off the MapReduce for determining and saving subscription counts."""
  job = mapreduce_pipeline.MapreducePipeline(
      'Count subscriptions',
      'offline_jobs.count_subscriptions_for_topic',
      'offline_jobs.save_subscription_counts_for_topic',
      'mapreduce.input_readers.DatastoreInputReader',
      mapper_params=dict(entity_kind='main.Subscription'),
      shards=4)
  # TODO(bslatkin): Pass through the queue name to run the job on. This is
  # a limitation in the mapper library.
  job.start()
  return job.pipeline_id
