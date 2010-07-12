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

"""Tests for the fork_join_queue module."""

import datetime
import logging
logging.basicConfig(format='%(levelname)-8s %(filename)s] %(message)s')
import os
import random
import sys
import unittest

import testutil
testutil.fix_path()

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import webapp

import fork_join_queue

################################################################################

class TestModel(db.Model):
  work_index = db.IntegerProperty()
  number = db.IntegerProperty()


TEST_QUEUE = fork_join_queue.ForkJoinQueue(
    TestModel,
    TestModel.work_index,
    '/path/to/my/task',
    'default',
    batch_size=3,
    batch_period_ms=2200,
    lock_timeout_ms=1000,
    sync_timeout_ms=250,
    stall_timeout_ms=30000,
    acquire_timeout_ms=50,
    acquire_attempts=20)


TEST_QUEUE_ZERO_BATCH_TIME = fork_join_queue.ForkJoinQueue(
    TestModel,
    TestModel.work_index,
    '/path/to/my/task',
    'default',
    batch_size=3,
    batch_period_ms=0,
    lock_timeout_ms=1000,
    sync_timeout_ms=250,
    stall_timeout_ms=30000,
    acquire_timeout_ms=50,
    acquire_attempts=20)


SHARDED_QUEUE = fork_join_queue.ShardedForkJoinQueue(
    TestModel,
    TestModel.work_index,
    '/path/to/my/task',
    'default-%(shard)s',
    batch_size=3,
    batch_period_ms=2200,
    lock_timeout_ms=1000,
    sync_timeout_ms=250,
    stall_timeout_ms=30000,
    acquire_timeout_ms=50,
    acquire_attempts=20,
    shard_count=4)


MEMCACHE_QUEUE = fork_join_queue.MemcacheForkJoinQueue(
    TestModel,
    TestModel.work_index,
    '/path/to/my/task',
    'default',
    batch_size=3,
    batch_period_ms=2200,
    lock_timeout_ms=1000,
    sync_timeout_ms=250,
    stall_timeout_ms=30000,
    acquire_timeout_ms=50,
    acquire_attempts=20,
    shard_count=4)


class ForkJoinQueueTest(unittest.TestCase):
  """Tests for the ForkJoinQueue class."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.setup_for_testing(require_indexes=False)
    self.now1 = 1274078068.886692
    self.now2 = 1274078097.79072
    self.gettime1 = lambda: self.now1
    self.gettime2 = lambda: self.now2
    os.environ['CURRENT_VERSION_ID'] = 'myversion.1234'
    if 'HTTP_X_APPENGINE_TASKNAME' in os.environ:
      del os.environ['HTTP_X_APPENGINE_TASKNAME']

  def expect_task(self,
                  index,
                  generation=0,
                  now_time=None,
                  cursor=None,
                  batch_period_ms=2200000):
    """Creates an expected task dictionary."""
    if now_time is None:
      now_time = self.now1
    gap_number = int(now_time / 30.0)
    import math
    work_item = {
      'name': 'fjq-TestModel-myversion-%d-%d-%d' % (
          gap_number, index, generation),
      # Working around weird rounding behavior of task queue stub.
      'eta': (10**6 * now_time) + batch_period_ms,
    }
    if cursor is not None:
      work_item['cursor'] = cursor
    return work_item

  def assertTasksEqual(self, expected_tasks, found_tasks,
                       check_eta=True):
    """Asserts two lists of tasks are equal."""
    found_tasks.sort(key=lambda t: t['eta'])
    for expected, found in zip(expected_tasks, found_tasks):
      self.assertEquals(expected['name'], found['name'])
      if check_eta:
        # Round these task ETAs to integers because the taskqueue stub does
        # not support floating-point ETAs.
        self.assertEquals(round(expected['eta'] / 10**6),
                          round(found['eta'] / 10**6))
      self.assertEquals(expected.get('cursor'),
                        found.get('params', {}).get('cursor'))
      self.assertEquals('POST', found['method'])
      self.assertEquals('/path/to/my/task', found['url'])
    self.assertEquals(len(expected_tasks), len(found_tasks))

  def testAddOne(self):
    """Tests adding a single entity to the queue."""
    t = TestModel(number=1)
    t.work_index = TEST_QUEUE.next_index()
    t.put()
    TEST_QUEUE.add(t.work_index, gettime=self.gettime1)
    self.assertTasksEqual(
        [self.expect_task(t.work_index)],
        testutil.get_tasks('default', usec_eta=True))

  def testAddMultiple(self):
    """Tests adding multiple tasks on the same index."""
    t1 = TestModel(number=1)
    t2 = TestModel(number=2)
    t3 = TestModel(number=3)

    tasks = [t1, t2, t3]
    work_index = TEST_QUEUE.next_index()
    for t in tasks:
      t.work_index = work_index
    db.put(tasks)
    TEST_QUEUE.add(work_index, gettime=self.gettime1)

    self.assertTasksEqual(
        [self.expect_task(work_index)],
        testutil.get_tasks('default', usec_eta=True))

  def testAddMultipleDifferent(self):
    """Tests adding multiple different tasks to different indexes."""
    t1 = TestModel(number=1)
    t2 = TestModel(number=2)
    t3 = TestModel(number=3)

    tasks1 = [t1, t2, t3]
    work_index = TEST_QUEUE.next_index()
    for t in tasks1:
      t.work_index = work_index
    db.put(tasks1)
    TEST_QUEUE.add(work_index, gettime=self.gettime1)

    memcache.incr('fjq-TestModel-index')

    t4 = TestModel(number=4)
    t5 = TestModel(number=5)
    t6 = TestModel(number=6)

    tasks2 = [t4, t5, t6]
    work_index2 = TEST_QUEUE.next_index()
    for t in tasks2:
      t.work_index = work_index2
    db.put(tasks2)
    TEST_QUEUE.add(work_index2, gettime=self.gettime2)

    self.assertNotEqual(work_index, work_index2)
    self.assertTasksEqual(
        [self.expect_task(work_index),
         self.expect_task(work_index2, now_time=self.now2)],
        testutil.get_tasks('default', usec_eta=True))

  def testAddAlreadyExists(self):
    """Tests when the added task already exists."""
    t = TestModel(number=1)
    t.work_index = TEST_QUEUE.next_index()
    t.put()
    TEST_QUEUE.add(t.work_index, gettime=self.gettime1)
    TEST_QUEUE.add(t.work_index, gettime=self.gettime1)

  def testNextIndexError(self):
    """Tests when the next index cannot be retrieved."""
    self.assertRaises(
        fork_join_queue.CannotGetIndexError,
        TEST_QUEUE.next_index,
        memget=lambda x: None)

  def testNextIndexBusyWait(self):
    """Tests busy waiting for a new index."""
    calls = [3]
    def incr(*args, **kwargs):
      result = memcache.incr(*args, **kwargs)
      memcache.incr(TEST_QUEUE.index_name)
      if calls[0] > 0:
        calls[0] -= 1
        return 100
      else:
        return result

    last_index = TEST_QUEUE.next_index(memincr=incr)
    for i in xrange(1, 4):
      self.assertEquals(
          TEST_QUEUE.FAKE_ZERO,
          int(memcache.get(TEST_QUEUE.add_counter_template %
                           fork_join_queue.knuth_hash(i))))
    self.assertEquals(
        TEST_QUEUE.FAKE_ZERO + 1,  # Extra 1 is for the writer lock!
        int(memcache.get(TEST_QUEUE.add_counter_template %
                         fork_join_queue.knuth_hash(4))))

  def testNextIndexBusyWaitFail(self):
    """Tests when busy waiting for a new index fails."""
    seen_keys = []
    def fake_incr(key, *args, **kwargs):
      seen_keys.append(key)
      return 100
    self.assertRaises(
        fork_join_queue.WriterLockError,
        TEST_QUEUE.next_index,
        memincr=fake_incr)
    self.assertEquals(
        (['fjq-TestModel-add-lock:2654435761'] * 20) +
        ['fjq-TestModel-index'],
        seen_keys)

  def testPopOne(self):
    """Tests popping a single entity from the queue."""
    t1 = TestModel(work_index=TEST_QUEUE.next_index(), number=1)

    # Ensure these other tasks don't interfere.
    t2 = TestModel(number=2)
    t3 = TestModel(number=3)
    db.put([t1, t2, t3])
    TEST_QUEUE.add(t1.work_index)

    request = testutil.create_test_request('POST', None)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = \
        self.expect_task(t1.work_index)['name']
    result = TEST_QUEUE.pop_request(request)

    self.assertEquals(1, len(result))
    self.assertEquals(t1.key(), result[0].key())
    self.assertEquals(t1.work_index, result[0].work_index)
    self.assertEquals(1, result[0].number)

  def testPopMultiple(self):
    """Tests popping multiple entities from a queue.

    Should also cause a continuation task to be inserted to handle entities
    after the current batch size.
    """
    work_index = TEST_QUEUE.next_index()
    tasks = []
    for i in xrange(6):
      tasks.append(TestModel(work_index=work_index, number=i))
    db.put(tasks)
    TEST_QUEUE.add(work_index, gettime=self.gettime1)

    # First pop request.
    request = testutil.create_test_request('POST', None)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = \
        self.expect_task(work_index)['name']
    result_list = TEST_QUEUE.pop_request(request)

    self.assertEquals(3, len(result_list))
    for i, result in enumerate(result_list):
      self.assertEquals(work_index, result.work_index)
      self.assertEquals(i, result.number)

    # Continuation one.
    next_task = testutil.get_tasks('default',
                                   expected_count=2,
                                   index=1,
                                   usec_eta=True)
    self.assertTrue('cursor' in next_task['params'])
    self.assertTrue(next_task['name'].endswith('-1'))

    request = testutil.create_test_request('POST', None,
                                           *next_task['params'].items())
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = next_task['name']
    result_list = TEST_QUEUE.pop_request(request)
    self.assertEquals(3, len(result_list))
    for i, result in enumerate(result_list):
      self.assertEquals(work_index, result.work_index)
      self.assertEquals(i + 3, result.number)

    # Continuation two.
    next_task = testutil.get_tasks('default',
                                   expected_count=3,
                                   index=2,
                                   usec_eta=True)
    next_params = next_task['params']
    self.assertTrue('cursor' in next_params)
    self.assertTrue(next_task['name'].endswith('-2'))

    request = testutil.create_test_request('POST', None,
                                           *next_task['params'].items())
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = next_task['name']
    result_list = TEST_QUEUE.pop_request(request)
    self.assertEquals([], result_list)
    testutil.get_tasks('default', expected_count=3, usec_eta=True)

  def testPopAlreadyExists(self):
    """Tests popping work items when the continuation task already exists."""
    work_index = TEST_QUEUE.next_index()
    tasks = []
    for i in xrange(6):
      tasks.append(TestModel(work_index=work_index, number=i))
    db.put(tasks)
    TEST_QUEUE.add(work_index, gettime=self.gettime1)
    testutil.get_tasks('default', expected_count=1)

    request = testutil.create_test_request('POST', None)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = \
        self.expect_task(work_index)['name']

    result_list = TEST_QUEUE.pop_request(request)
    testutil.get_tasks('default', expected_count=2)

    result_list = TEST_QUEUE.pop_request(request)
    testutil.get_tasks('default', expected_count=2)

  def testIncrementIndexFail(self):
    """Tests when the reader-writer lock is lost."""
    work_index = TEST_QUEUE.next_index()
    memcache.delete(TEST_QUEUE.add_counter_template % work_index)
    self.assertTrue(TEST_QUEUE._increment_index(work_index))

  def testIncrementIndexBusyWait(self):
    """Tests busy waiting for the writer lock."""
    work_index = TEST_QUEUE.next_index()
    self.assertFalse(TEST_QUEUE._increment_index(work_index))

  def testZeroBatchTime(self):
    """Tests that zero batch time results in no task ETA."""
    work_index = TEST_QUEUE_ZERO_BATCH_TIME.next_index()
    task = TestModel(work_index=work_index, number=1)
    db.put(task)

    before_index = memcache.get(TEST_QUEUE_ZERO_BATCH_TIME.index_name)
    self.assertEquals(
        work_index, fork_join_queue.knuth_hash(before_index))
    TEST_QUEUE_ZERO_BATCH_TIME.add(work_index, gettime=self.gettime1)

    # This confirms the behavior that batch_period_ms of zero will cause
    # immediate increment after adding the tasks.
    after_index = memcache.get(TEST_QUEUE_ZERO_BATCH_TIME.index_name)
    self.assertEquals(before_index + 1, after_index)

    self.assertTasksEqual(
      [self.expect_task(work_index)],
      testutil.get_tasks('default', usec_eta=True),
      check_eta=False)

  def testShardedQueue(self):
    """Tests adding and popping from a sharded queue with continuation."""
    from google.appengine.api import apiproxy_stub_map
    stub = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
    old_valid = stub._IsValidQueue
    stub._IsValidQueue = lambda *a, **k: True
    try:
      work_index = SHARDED_QUEUE.next_index()
      tasks = []
      for i in xrange(5):
        tasks.append(TestModel(work_index=work_index, number=i))
      db.put(tasks)
      SHARDED_QUEUE.add(work_index, gettime=self.gettime1)
      queue_name = 'default-%d' % (1 + (work_index % 4))
      testutil.get_tasks(queue_name, expected_count=1)

      # First pop request.
      request = testutil.create_test_request('POST', None)
      os.environ['HTTP_X_APPENGINE_TASKNAME'] = \
          self.expect_task(work_index)['name']
      result_list = SHARDED_QUEUE.pop_request(request)

      self.assertEquals(3, len(result_list))
      for i, result in enumerate(result_list):
        self.assertEquals(work_index, result.work_index)
        self.assertEquals(i, result.number)

      # Continuation one.
      next_task = testutil.get_tasks(queue_name,
                                     expected_count=2,
                                     index=1)
      self.assertTrue('cursor' in next_task['params'])
      self.assertTrue(next_task['name'].endswith('-1'))
    finally:
      stub._IsValidQueue = old_valid

  def testMemcacheQueue(self):
    """Tests adding and popping from an in-memory queue with continuation."""
    work_index = MEMCACHE_QUEUE.next_index()
    work_items = [TestModel(key=db.Key.from_path(TestModel.kind(), i),
                            work_index=work_index, number=i)
                  for i in xrange(1, 6)]
    MEMCACHE_QUEUE.put(work_index, work_items)
    MEMCACHE_QUEUE.add(work_index, gettime=self.gettime1)
    testutil.get_tasks('default', expected_count=1)

    # First pop request.
    request = testutil.create_test_request('POST', None)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = \
        self.expect_task(work_index)['name']
    result_list = MEMCACHE_QUEUE.pop_request(request)

    self.assertEquals(3, len(result_list))
    for i, result in enumerate(result_list):
      self.assertEquals(work_index, result.work_index)
      self.assertEquals(i + 1, result.number)

    # Continuation task enqueued.
    next_task = testutil.get_tasks('default',
                                   expected_count=2,
                                   index=1)
    self.assertEquals(3, int(next_task['params']['cursor']))
    self.assertTrue(next_task['name'].endswith('-1'))

    # Second pop request.
    request = testutil.create_test_request(
        'POST', None, *next_task['params'].items())
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = next_task['name']
    result_list = MEMCACHE_QUEUE.pop_request(request)

    self.assertEquals(2, len(result_list))
    for i, result in enumerate(result_list):
      self.assertEquals(work_index, result.work_index)
      self.assertEquals(i + 4, result.number)

  def testMemcacheQueue_IncrError(self):
    """Tests calling put() when memcache increment fails."""
    work_index = MEMCACHE_QUEUE.next_index()
    entity = TestModel(work_index=work_index, number=0)
    self.assertRaises(fork_join_queue.MemcacheError,
                      MEMCACHE_QUEUE.put,
                      work_index, [entity],
                      memincr=lambda *a, **k: None)

  def testMemcacheQueue_PutSetError(self):
    """Tests calling put() when memcache set fails."""
    work_index = MEMCACHE_QUEUE.next_index()
    entity = TestModel(work_index=work_index, number=0)
    self.assertRaises(fork_join_queue.MemcacheError,
                      MEMCACHE_QUEUE.put,
                      work_index, [entity],
                      memset=lambda *a, **k: ['blah'])

  def testMemcacheQueue_PopError(self):
    """Tests calling pop() when memcache is down."""
    work_index = MEMCACHE_QUEUE.next_index()
    entity = TestModel(work_index=work_index, number=0)
    MEMCACHE_QUEUE.put(work_index, [entity])
    memcache.flush_all()

    request = testutil.create_test_request('POST', None)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = \
        self.expect_task(work_index)['name']
    result_list = MEMCACHE_QUEUE.pop_request(request)
    self.assertEquals([], result_list)

  def testMemcacheQueue_PopHoles(self):
    """Tests when there are holes in the memcache array."""
    work_index = MEMCACHE_QUEUE.next_index()
    work_items = [TestModel(key=db.Key.from_path(TestModel.kind(), i),
                            work_index=work_index, number=i)
                  for i in xrange(1, 6)]
    MEMCACHE_QUEUE.put(work_index, work_items)
    memcache.delete(MEMCACHE_QUEUE._create_index_key(work_index, 1))

    request = testutil.create_test_request('POST', None)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = \
        self.expect_task(work_index)['name']
    result_list = MEMCACHE_QUEUE.pop_request(request)
    self.assertEquals([1, 3], [r.number for r in result_list])

  def testMemcacheQueue_PopDecodeError(self):
    """Tests when proto decoding fails on the pop() call."""
    work_index = MEMCACHE_QUEUE.next_index()
    work_items = [TestModel(key=db.Key.from_path(TestModel.kind(), i),
                            work_index=work_index, number=i)
                  for i in xrange(1, 6)]
    MEMCACHE_QUEUE.put(work_index, work_items)

    memcache.set(MEMCACHE_QUEUE._create_index_key(work_index, 1), 'bad data')
    request = testutil.create_test_request('POST', None)
    os.environ['HTTP_X_APPENGINE_TASKNAME'] = \
        self.expect_task(work_index)['name']
    result_list = MEMCACHE_QUEUE.pop_request(request)

################################################################################

if __name__ == '__main__':
  unittest.main()
