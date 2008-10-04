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
import random
import wsgiref.handlers
from google.appengine.ext import webapp
from google.appengine.ext import db


class SomeUpdate(db.Model):
  """Some topic update."""
  the_update = db.TextProperty(required=True)
  update_time = db.DateTimeProperty(auto_now_add=True)


class LogEchoHandler(webapp.RequestHandler):
  """Echos all input data to the log."""

  def post(self):
    some_update = SomeUpdate(the_update=self.request.get('content'))
    some_update.put()
    self.response.set_status(200)
    self.response.out.write("Aight.  Saved.");

  def get(self):
    self.response.set_status(200)
    self.response.out.write("""
        <html>
        <head>
           <script src='/static/agg.js'></script>
        </head>
        <body>
           <h1>Subscriber's Aggregator Page:</h1>
           <div id='content'></div>
        </body>
        </html>
    """)


class ItemsHandler(webapp.RequestHandler):
  """Gets the items."""

  def get(self):
    for update in SomeUpdate.gql('ORDER BY update_time DESC').fetch(50):
      self.response.out.write("""<p>Tis: %s</p>""" % update.the_update)


application = webapp.WSGIApplication(
  [
    (r'/items', ItemsHandler),
    (r'/.*', LogEchoHandler),
  ],
  debug=True)


def main():
  wsgiref.handlers.CGIHandler().run(application)


if __name__ == '__main__':
  main()
