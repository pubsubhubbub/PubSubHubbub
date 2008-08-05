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

import wsgiref.handlers

from google.appengine.api import apiproxy_stub_map
from google.appengine.runtime import apiproxy_errors

class AsyncAPIProxy:
  def __init__(self):
    self.enqueued = []

  def start_call(self, service, method, pbrequest, pbresponse, callback):
    """Callback is ->(pbresponse | None, None | Exception)"""
    self.enqueued.append((service, method, pbrequest, pbresponse, callback))

  def rpcs_outstanding(self):
    return len(self.enqueued);

  def wait(self):
    """Wait for RPCs to finish.  Returns true if one was processed."""
    if not self.rpcs_outstanding():
      return False
    (service, method, pbrequest, pbresponse, callback) = self.enqueued.pop(0);
    try:
      apiproxy_stub_map.MakeSyncCall(service, method, pbrequest, pbresponse)
      callback(pbresponse, None)
    except apiproxy_errors.ApplicationError, e:
      callback(None, e)
    return True
