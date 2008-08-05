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

import collections

from google.appengine.api import apiproxy_stub_map
from google.appengine.runtime import apiproxy
from google.appengine.runtime import apiproxy_errors


class DevAppServerRPC(apiproxy.RPC):
  def MakeCall(self):
    pass
  
  def Wait(self):
    pass
  
  def CheckSuccess(self):
    apiproxy_stub_map.MakeSyncCall(self.package, self.call,
                                   self.request, self.response)


try:
  from google3.apphosting.runtime import _apphosting_runtime___python__apiproxy
  AsyncRPC = DevAppServerRPC
except ImportError:
  AsyncRPC = apiproxy.RPC


class AsyncAPIProxy(object):
  def __init__(self):
    self.enqueued = collections.deque()

  def start_call(self, package, call, pbrequest, pbresponse, user_callback):
    """user_callback is a callback that takes (response, exception)"""
    if not callable(user_callback):
      raise TypeError('%r not callable' % user_callback)

    rpc = AsyncRPC(package, call, pbrequest, pbresponse,
                   lambda: user_callback(pbresponse, None))
    setattr(rpc, 'user_callback', user_callback) # TODO make this pretty
    self.enqueued.append(rpc)
    rpc.MakeCall()

  def rpcs_outstanding(self):
    return len(self.enqueued)

  def wait_one(self):
    """Wait for a single RPC to finish. Returns True if one was processed."""
    if not self.enqueued:
      return False
    
    rpc = self.enqueued.popleft()
    rpc.Wait()
    try:
      rpc.CheckSuccess()
      rpc.user_callback(rpc.response, None)
    except (apiproxy.Error, apiproxy_errors.ApplicationError), e:
      rpc.user_callback(None, e)
    return True

  def wait(self):
    """Wait for RPCs to finish.  Returns True if any were processed."""
    while self.enqueued:
      self.wait_one()
    else:
      return False
    return True
