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

import feedparser
import simplejson

class SomeUpdate(db.Model):
  """Some topic update."""
  title = db.TextProperty(required=True)
  content = db.TextProperty(required=True)
  updated = db.StringProperty(required=True)  # ISO 8601


class InputHandler(webapp.RequestHandler):
  """Handles feed input."""

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

  def post(self):
    content = unicode(self.request.get('content')).encode('utf-8')
    data = feedparser.parse(content)
    if data.bozo:
      logging.error('Bozo feed data on line %d, message %s',
                     data.bozo_exception.getLineNumber(),
                     data.bozo_exception.getMessage())
      return

    update_list = []
    for entry in data.entries:
      # TODO: Do something smarter than use the update time as the key name.
      update_list.append(SomeUpdate(
          key_name='key_' + entry.updated,
          title=entry.title,
          content=entry.content[0].value,
          updated=entry.updated))
    db.put(update_list)
    self.response.set_status(200)
    self.response.out.write("Aight.  Saved.");


class DebugHandler(webapp.RequestHandler):
  """Debug handler for simulating events."""
  def get(self):
    self.response.out.write("""
      <html>
      <body>
      <form action="/" method="post">
        <div>Simulate feed:</div>
        <textarea name="content" cols="40" rows="40"></textarea>
        <div><input type="submit" value="submit"></div>
      </form>
      </body>
      </html>
      """)


class ItemsHandler(webapp.RequestHandler):
  """Gets the items."""

  def get(self):
    encoder = simplejson.JSONEncoder()
    stuff = []
    for update in SomeUpdate.gql('ORDER BY updated DESC').fetch(50):
      stuff.append({'time': update.updated,
                    'title': update.title,
                    'content': update.content})
    self.response.out.write(encoder.encode(stuff))


application = webapp.WSGIApplication(
  [
    (r'/items', ItemsHandler),
    (r'/debug', DebugHandler),
    (r'/.*', InputHandler),
  ],
  debug=True)


def main():
  wsgiref.handlers.CGIHandler().run(application)


if __name__ == '__main__':
  main()
