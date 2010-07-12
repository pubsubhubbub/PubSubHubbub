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

"""Fork-join queue for App Engine.

The Task Queue API executes tasks in a push manner instead of polling with
visibility time like Amazon SQS and other workqueue systems. However, often
you may need to process multiple pieces of queued work simultaneously in a
single App Engine request; the benefit being that you can minimize the
impact of high latency API calls that block and occupy a thread by doing many
asynchronous calls in parallel.

Fork-join queues have three important parameters:

  * Batch Time: How often new task entities added to the queue should be
      coalesced to run as a single unit in parallel. This should be low
      enough to not drastically affect latency, but high enough that its
      batching effects result in fewer overall occupied threads.

  * Batch Size: How many task entities to run at a time in a single request.
      This should be tuned for your asynchronous work's maximum wall-clock time
      and the maximum asynchronous API calls need to do in parallel.

  * Shard count: (optional) How many parallel shards to use for this queue.
      This represents the minimum parallelism you'll see since you won't get
      coalescing until you have at least as many tasks as shards.

How it works:

1. Incoming, Datastore entities representing work items are assigned an index
number and committed. A shard number is assigned for load-balancing based on
the index assigned.

2. After the work entities are committed to the Datastore, corresponding
push-oriented taskqueue tasks are put on the push queue. These push tasks have
an ETA of the next highest time interval for the fork-queue based on the batch
time. The magical part here is that many task entities in the same batch time
will "dedupe" their push-task enqueueing by getting a tombstone/exists error
because they have overlapping task names (based on the work index). Thus, many
separate physical tasks entities will *fan-in* to a single logical task.

3. The push task runs *after* all work item entities have been written to the
Datastore (guaranteed with a reader/writer lock). The task queries for work in
its particular work index region. It then handles these tasks (in user code)
and allows the task to complete. The task entities need not be deleted.

4. (optional) When tasks are popped from the fork-join queue, a continuation
task will be enqueued immediately after the batch size is received to do
more work in parallel in smaller chunk sizes.


Obligatory diagram (where numbers correspond to batch generations):

|---------|---------|---> time
 ^ ^ ^ ^  ^  ^ ^ ^  ^
 1 1 1 1  R  2 2 2  R
          u         u
          n         n
          1         2


Nota Bene: A naive approach to pull-oriented queues (constantly query on an
'eta' parameter sorting by 'eta' descending, then delete finished entities) may
result in poor performance because of how the Datastore's garbage collection
interacts with Datastore queries and Bigtable's tablet splitting behavior.
Using contiguous row indexes on any work item properties can have the same
effect, so a hash of the sequential work index is used to ensure balancing
across tablets.
"""

import datetime
import logging
import os
import random
import time

from google.net.proto import ProtocolBuffer
from google.appengine.api import memcache
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db

# TODO: Consider using multiple work indexes to alleviate the memcache
# hotspot for the writer path.

################################################################################

def knuth_hash(number):
  """A decent hash function for integers."""
  return (number * 2654435761) % 2**32


def datetime_from_stamp(stamp):
  """Converts a UNIX timestamp to a datetime.datetime including microseconds."""
  result = datetime.datetime.utcfromtimestamp(stamp)
  result += datetime.timedelta(microseconds=10**6 * (stamp - int(stamp)))
  return result


class Error(Exception):
  """Base-class for exceptions in this module."""

class WriterLockError(Error):
  """When the task adder could not increment the writer lock."""

class CannotGetIndexError(Error):
  """When the task adder could not get a starting index from memcache."""

class TaskConflictError(Error):
  """The added task has already ran, meaning the work index is invalid."""

class MemcacheError(Error):
  """Enqueuing the work item in memcache failed."""


class ForkJoinQueue(object):
  """A fork-join queue for App Engine."""

  FAKE_ZERO = 2**16
  LOCK_OFFSET = FAKE_ZERO / 2

  def __init__(self,
               model_class,
               index_property,
               task_path,
               queue_name,
               batch_size=None,
               batch_period_ms=None,
               lock_timeout_ms=None,
               sync_timeout_ms=None,
               stall_timeout_ms=None,
               acquire_timeout_ms=None,
               acquire_attempts=None):
    """Initializer.

    Args:
      model_class: The model class for work items.
      index_property: The model class's property for work indexes.
      task_path: Path where joined tasks should run.
      queue_name: Queue on which joined tasks should run.
      batch_size: How many work items to process at a time before spawning
        another task generation to handle more.
      batch_period_ms: How often, in milliseconds, to batch work items.
      lock_timeout_ms: How long to wait, in milliseconds, for all writers
        before a joined task executes.
      sync_timeout_ms: How long it takes, in milliseconds, for writers to
        finish enqueueing work before readers should attempt to acquire the
        lock again.
      stall_timeout_ms: How often task queue naming overlaps should be
        rotated, in milliseconds, in order to prevent the queue stall caused
        by memcache outages.
      acquire_timeout_ms: How long to wait, in milliseconds, for writers to
        acquire a new index on each attempt.
      acquire_attempts: How many times writers should attempt to get new
        indexes before raising an error.
    """
    # TODO: Add validation.
    self.model_class = model_class
    self.name = 'fjq-' + model_class.kind()
    self.index_property = index_property
    self.task_path = task_path
    self.queue_name = queue_name
    self.batch_size = batch_size
    self.lock_timeout = lock_timeout_ms / 1000.0
    self.sync_attempts = int(1.0 * lock_timeout_ms / sync_timeout_ms)
    self.sync_timeout = sync_timeout_ms / 1000.0
    self.stall_timeout = stall_timeout_ms / 1000.0
    self.acquire_timeout = acquire_timeout_ms / 1000.0
    self.acquire_attempts = acquire_attempts
    if batch_period_ms == 0:
      self.batch_delta = None
    else:
      self.batch_delta = datetime.timedelta(microseconds=batch_period_ms * 1000)

  def get_queue_name(self, index):
    """Returns the name of the queue to use based on the given work index."""
    return self.queue_name

  @property
  def lock_name(self):
    """Returns the lock key prefix for the current prefix name."""
    return self.name + '-lock'

  @property
  def add_counter_template(self):
    """Returns the add counter prefix template for the current prefix name."""
    return self.name + '-add-lock:%d'

  @property
  def index_name(self):
    """Returns the index key prefix for the current prefix name."""
    return self.name + '-index'

  def next_index(self,
                 memget=memcache.get,
                 memincr=memcache.incr,
                 memdecr=memcache.decr):
    """Reserves the next work index.

    Args:
      memget, memincr, memdecr: Used for testing.

    Returns:
      The next work index to use for work.
    """
    for i in xrange(self.acquire_attempts):
      next_index = memget(self.index_name)
      if next_index is None:
        memcache.add(self.index_name, 1)
        next_index = memget(self.index_name)
        if next_index is None:
          # Can't get it or add it, which means memcache is probably down.
          # Handle this as a separate fast-path to prevent memcache overload
          # during memcache failures.
          raise CannotGetIndexError(
              'Cannot establish new task index in memcache.')

      next_index = knuth_hash(int(next_index))
      add_counter = self.add_counter_template % next_index
      count = memincr(add_counter, 1, initial_value=self.FAKE_ZERO)
      if count < self.FAKE_ZERO:
        # When the counter is super negative that means this index has been
        # locked and we can no longer add tasks to it. We need to "refund" the
        # reader lock we took to ensure the worker doesn't wait for it.
        memdecr(add_counter, 1)
      else:
        return next_index
      time.sleep(self.acquire_timeout)
    else:
      # Force the index forward; here we're stuck in a loop where the memcache
      # index was evicted and all new lock acqusitions are reusing old locks
      # that were already closed off to new writers.
      memincr(self.index_name)
      raise WriterLockError('Task adder could not increment writer lock.')

  def add(self, index, gettime=time.time):
    """Adds a task for a work index, decrementing the writer lock."""
    now_stamp = gettime()
    # Nearest gap used to kickstart the queues when a task is dropped or
    # memcache is evicted. This prevents new task names from overlapping with
    # old ones.
    nearest_gap = int(now_stamp / self.stall_timeout)
    # Include major version in the task name to ensure that test tasks
    # enqueued from a non-default major version will run in the new context
    # instead of the default major version.
    major_version, minor_version = os.environ['CURRENT_VERSION_ID'].split('.')
    task_name = '%s-%s-%d-%d-%d' % (
        self.name, major_version, nearest_gap, index, 0)

    # When the batch_period_ms is zero, then there should be no ETA, the task
    # should run immediately and the reader will busy wait for all writers.
    if self.batch_delta is None:
      eta = None
    else:
      eta = datetime_from_stamp(now_stamp) + self.batch_delta

    try:
      taskqueue.Task(
        method='POST',
        name=task_name,
        url=self.task_path,
        eta=eta
      ).add(self.get_queue_name(index))
      if self.batch_delta is None:
        # When the batch_period_ms is zero, we want to immediately move the
        # index to the next position as soon as the current batch finishes
        # writing its task. This will only run for the first successful task
        # inserter.
        memcache.incr(self.index_name)
    except taskqueue.TaskAlreadyExistsError:
      # This is okay. It means the task has already been inserted by another
      # add() call for this same batch. We're holding the lock at this point
      # so we know that job won't start yet.
      pass
    except taskqueue.TombstonedTaskError, e:
      # This is bad. This means 1) the lock we held expired and the task already
      # ran, 2) this task name somehow overlaps with an old task. Return the
      # error to the caller so they can try again.
      raise TaskConflictError('Task named tombstoned: %s' % e)
    finally:
      # Don't bother checking the decr status; worst-case the worker job
      # will time out after some number of seconds and proceed anyways.
      memcache.decr(self.add_counter_template % index, 1)

  def _increment_index(self, last_index):
    """Moves the work index forward and waits for all writers.

    Args:
      last_index: The last index that was used for the reader/writer lock.

    Returns:
      True if all writers were definitely finished; False if the reader/writer
      lock timed out and we are proceeding anyways.
    """
    # Increment the batch index counter so incoming jobs will use a new index.
    # Don't bother setting an initial value here because next_index() will
    # do this when it notices no current index is present. Do this *before*
    # closing the reader/writer lock below to decrease active writers on the
    # current index.
    # We do this even in the case that batch_period_ms was zero, just in case
    # that memcache operation failed for some reason, we'd rather have more
    # batches then have the work index pipeline stall.
    memcache.incr(self.index_name)

    # Prevent new writers by making the counter extremely negative. If the
    # decrement fails here we can't recover anyways, so just let the worker go.
    add_counter = self.add_counter_template % last_index
    memcache.decr(add_counter, self.LOCK_OFFSET)

    for i in xrange(self.sync_attempts):
      counter = memcache.get(add_counter)
      # Less than or equal LOCK_OFFSET here in case a writer decrements twice
      # due to rerunning failure tasks.
      if counter is None or int(counter) <= self.LOCK_OFFSET:
        # Worst-case the counter will be gone due to memcache eviction, which
        # means the worker can procede with without waiting for writers
        # and just process whatever it can find. This may drop some work.
        return True
      time.sleep(self.sync_timeout)
    else:
      logging.critical('Worker for %s gave up waiting for writers', self.name)

    return False

  def _query_work(self, index, cursor):
    """Queries for work in the Datastore."""
    query = (self.model_class.all()
        .filter('%s =' % self.index_property.name, index)
        .order('__key__'))
    if cursor:
      query.with_cursor(cursor)
    result_list = query.fetch(self.batch_size)
    return result_list, query.cursor()

  def pop_request(self, request):
    """Pops work to be done based on a task queue request.

    Args:
      request: webapp.Request with the task payload.

    Returns:
      A list of work items, if any.
    """
    # TODO: Use request.headers['X-AppEngine-TaskName'] instead of environ.
    return self.pop(os.environ['HTTP_X_APPENGINE_TASKNAME'],
                    request.get('cursor'))

  def pop(self, task_name, cursor=None):
    """Pops work to be done based on just the task name.

    Args:
      task_name: The name of the task.
      cursor: The value of the cursor for this task (optional).

    Returns:
      A list of work items, if any.
    """
    rest, index, generation = task_name.rsplit('-', 2)
    index, generation = int(index), int(generation)

    if not cursor:
      # The root worker task already waited for all writers, so continuation
      # tasks can start processing immediately.
      self._increment_index(index)

    result_list, cursor = self._query_work(index, cursor)

    if len(result_list) == self.batch_size:
      for i in xrange(3):
        try:
          taskqueue.Task(
            method='POST',
            name='%s-%d-%d' % (rest, index, generation + 1),
            url=self.task_path,
            params={'cursor': cursor}
          ).add(self.get_queue_name(index))
          break
        except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
          # This means the continuation chain already started and this root
          # task failed for some reason; no problem.
          break
        except (taskqueue.TransientError, taskqueue.InternalError):
          # Ignore transient taskqueue errors.
          if i == 2:
            raise

    return result_list


class ShardedForkJoinQueue(ForkJoinQueue):
  """A fork-join queue that shards actual work across multiple task queues."""

  def __init__(self, *args, **kwargs):
    """Initialized.

    Args:
      *args, **kwargs: Passed to ForkJoinQueue.
      shard_count: How many queues there are for sharding the incoming work.
    """
    self.shard_count = kwargs.pop('shard_count')
    ForkJoinQueue.__init__(self, *args, **kwargs)

  def get_queue_name(self, index):
    return self.queue_name % {'shard': 1 + (index % self.shard_count)}


class MemcacheForkJoinQueue(ShardedForkJoinQueue):
  """A fork-join queue that only stores work items in memcache.

  To use, call next_index() to get the work index then call the put() method,
  passing one or more model instances to enqueued in memcache.

  Also a sharded queue for maximum throughput.
  """

  def __init__(self, *args, **kwargs):
    """Initializer.

    Args:
      *args, **kwargs: Passed to ShardedForkJoinQueue.
      expiration_seconds: How long items inserted into memcache should remain
        until they are evicted due to timeout. Default is 0, meaning they
        will never be evicted.
    """
    if 'expiration_seconds' in kwargs:
      self.expiration_seconds = kwargs.pop('expiration_seconds')
    else:
      self.expiration_seconds = 0
    ShardedForkJoinQueue.__init__(self, *args, **kwargs)

  def _create_length_key(self, index):
    """Creates a length memecache key for the length of the in-memory queue."""
    return '%s:length:%d' % (self.name, index)

  def _create_index_key(self, index, number):
    """Creates an index memcache key for the given in-memory queue location."""
    return '%s:index:%d-%d' % (self.name, index, number)

  def put(self,
          index,
          entity_list,
          memincr=memcache.incr,
          memset=memcache.set_multi):
    """Enqueue a model instance on this queue.

    Does not write to the Datastore.

    Args:
      index: The work index for this entity.
      entity_list: List of work entities to insert into the in-memory queue.
      memincr, memset: Used for testing.

    Raises:
      MemcacheError if the entities were not successfully added.
    """
    length_key = self._create_length_key(index)
    end = memincr(length_key, len(entity_list), initial_value=0)
    if end is None:
      raise MemcacheError('Could not increment length key %r' % length_key)

    start = end - len(entity_list)
    key_map = {}
    for number, entity in zip(xrange(start, end), entity_list):
      key_map[self._create_index_key(index, number)] = db.model_to_protobuf(
          entity)

    result = memset(key_map, time=self.expiration_seconds)
    if result:
      raise MemcacheError('Could not set memcache keys %r' % result)

  def _query_work(self, index, cursor):
    """Queries for work in memcache."""
    if cursor:
      try:
        cursor = int(cursor)
      except ValueError:
        # This is an old style task that resides in the Datastore, not
        # memcache. Use the parent implementation instead.
        return super(MemcacheForkJoinQueue, self)._query_work(index, cursor)
    else:
      cursor = 0

    key_list = [self._create_index_key(index, n)
                for n in xrange(cursor, cursor + self.batch_size)]
    results = memcache.get_multi(key_list)

    result_list = []
    for key in key_list:
      proto = results.get(key)
      if not proto:
        continue
      try:
        result_list.append(db.model_from_protobuf(proto))
      except ProtocolBuffer.ProtocolBufferDecodeError:
        logging.exception('Could not decode EntityPb at memcache key %r: %r',
                          key, proto)

    return result_list, cursor + self.batch_size
