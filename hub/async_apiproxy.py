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

"""APIProxy-like object that enables asynchronous API calls."""

import collections
import logging
import sys

from google.appengine.api import apiproxy_stub_map
from google.appengine.runtime import apiproxy
from google.appengine.runtime import apiproxy_errors
from google3.apphosting.runtime import _apphosting_runtime___python__apiproxy


class DevAppServerRPC(apiproxy.RPC):
  """RPC-like object for use in the dev_appserver environment."""

  def MakeCall(self):
    pass

  def Wait(self):
    pass

  def CheckSuccess(self):
    apiproxy_stub_map.MakeSyncCall(self.package, self.call,
                                   self.request, self.response)
    self.callback()


if hasattr(_apphosting_runtime___python__apiproxy, 'MakeCall'):
  AsyncRPC = apiproxy.RPC
  logging.debug('Using apiproxy.RPC')
else:
  logging.debug('Using DevAppServerRPC')
  AsyncRPC = DevAppServerRPC


class AsyncAPIProxy(object):
  """Proxy for asynchronous API calls."""

  def __init__(self):
    # TODO: Randomize this queue in the dev_appserver to simulate a real
    # asynchronous queue and better catch any funny race-conditions or
    # unclear event ordering dependencies.
    self.enqueued = collections.deque()
    self.complete = collections.deque()

  def start_call(self, package, call, pbrequest, pbresponse, user_callback,
                 deadline=None):
    """user_callback is a callback that takes (response, exception)"""
    if not callable(user_callback):
      raise TypeError('%r not callable' % user_callback)

    # Do not actually supply the callback to the async call function because
    # when it runs it could interfere with global state (like Datastore
    # transactions). The callback will be run from the wait_one() function.
    done_callback = lambda: user_callback(pbresponse, None)
    rpc = AsyncRPC(package, call, pbrequest, pbresponse,
                   lambda: self.end_call(done_callback),
                   deadline=deadline)
    setattr(rpc, 'user_callback', user_callback)
    setattr(rpc, 'pbresponse', pbresponse)

    self.enqueued.append(rpc)
    show_request = '...'
    if rpc.package == 'urlfetch':
      show_request = pbrequest.url()
    logging.debug('Making call for RPC(%s, %s, %s, ..)',
                  rpc.package, rpc.call, show_request)
    rpc.MakeCall()

  def end_call(self, rpc):
    """An outstanding RPC has completed, enqueue its callback for execution."""
    self.complete.append(rpc)

  def rpcs_outstanding(self):
    """Returns the number of asynchronous RPCs pending in this proxy."""
    return len(self.enqueued)

  def _wait_one(self):
    """Wait for a single RPC to finish."""
    if not self.enqueued:
      return
    rpc = self.enqueued.popleft()
    logging.debug('Waiting for RPC(%s, %s, .., ..)', rpc.package, rpc.call)
    rpc.Wait()
    try:
      rpc.CheckSuccess()
    except (apiproxy_errors.Error, apiproxy_errors.ApplicationError), e:
      rpc.user_callback(None, e)

  def _run_callbacks(self):
    """Runs a single RPC's success callback.

    Callbacks are run in a loop like this from the wait() callstack to avoid
    race conditions from the APIProxy. Any API call can cause asynchronous
    callbacks to fire before the main thread goes to sleep, which means a
    user callback could run *before* a Commit() call finishes; this causes
    really bad situations when the user callback also does some API calls. To
    handle this properly, all callbacks will just go onto the completion
    queue, and then run at the top of the stack here after at least one RPC
    waiting period has finished.
    """
    while True:
      try:
        callback = self.complete.popleft()
      except IndexError:
        return
      else:
        callback()

  def wait(self):
    """Wait for RPCs to finish. Returns True if any were processed."""
    while self.enqueued or self.complete:
      # Run the callbacks before even waiting, because a response could have
      # come back during any outbound API call.
      self._run_callbacks()
      self._wait_one()
