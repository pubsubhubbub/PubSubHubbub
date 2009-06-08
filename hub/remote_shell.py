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

import testutil
testutil.fix_path()

from main import *

import code
import getpass
import sys

from google.appengine.ext.remote_api import remote_api_stub
from google.appengine.ext import db

def auth_func():
  return raw_input('Username:'), getpass.getpass('Password:')

if len(sys.argv) > 3:
  print "Usage: %s [app_id] [host]" % (sys.argv[0],)
  sys.exit(1)

app_id = 'pubsubhubbub'
if len(sys.argv) >= 2:
  app_id = sys.argv[1]

host = '%s.appspot.com' % app_id
if len(sys.argv) == 3:
  host = sys.argv[2]

remote_api_stub.ConfigureRemoteDatastore(app_id, '/remote_api', auth_func, host)

code.interact('App Engine interactive console for %s' % app_id, None, locals())
