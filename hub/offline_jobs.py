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
import time

from google.appengine.ext import db

import main

from mapreduce import context
from mapreduce import operation as op


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
      params = context.get().mapreduce_spec.params
      self.threshold_timestamp = datetime.datetime.utcfromtimestamp(
          params['threshold_timestamp'])

    if sub.expiration_time < self.threshold_timestamp:
      sub.request_insert(sub.callback, sub.topic, sub.verify_token,
                         sub.secret, auto_reconfirm=True)
