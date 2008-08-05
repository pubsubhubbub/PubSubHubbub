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

from google.appengine.api import urlfetch
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import urlfetch_service_pb
from google.appengine.api.urlfetch_errors import *
from google.appengine.ext import webapp
from google.appengine.runtime import apiproxy_errors

import urlfetch_async
import async_apiproxy

async_proxy = async_apiproxy.AsyncAPIProxy()

class MainHandler(webapp.RequestHandler):

  def get(self):
    self.response.out.write('Hello world!')
    # start async fetch:
    urlfetch_async.fetch("http://bradfitz.com/test/1.txt", async_proxy=async_proxy, callback=self.on_url)
    urlfetch_async.fetch("http://bradfitz.com/test/2.txt", async_proxy=async_proxy, callback=self.on_url)
    async_proxy.wait()

  def on_url(self, result, exception):
    # response = urlfetch_service_pb.URLFetchResponse
    if result:
      self.response.out.write("<p>Got content: " + result.content + "</p>\n")
    else:
      self.response.out.write("Got exception!")


  

def main():
  application = webapp.WSGIApplication([('/', MainHandler)],
                                       debug=True)
  wsgiref.handlers.CGIHandler().run(application)



if __name__ == '__main__':
  main()
