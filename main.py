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

import logging
logging.getLogger().setLevel(logging.DEBUG)
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
    self.start_async_fetch("http://bradfitz.com/test/1.txt")
    self.start_async_fetch("http://bradfitz.com/test/2.txt")
    async_proxy.wait()

  def start_async_fetch(self, url):
    def callback(result, exception):
      self.on_url(url, result, exception)
    urlfetch_async.fetch(url, async_proxy=async_proxy, callback=callback)

  def on_url(self, url, result, exception):
    logging.info('Callback received for "%s": %s', url, result.status_code)
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
