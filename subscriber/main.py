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

"""Simple subscriber that aggregates all feeds together."""

import hashlib
import logging
import random
import wsgiref.handlers
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template

import feedparser
import simplejson


class SomeUpdate(db.Model):
  """Some topic update.
  
  Key name will be a hash of the feed source and item ID.
  """
  title = db.TextProperty(required=True)
  content = db.TextProperty(required=True)
  updated = db.StringProperty(required=True)  # ISO 8601 date
  source = db.TextProperty(required=True)


class InputHandler(webapp.RequestHandler):
  """Handles feed input and subscription"""

  def get(self):
    # Just subscribe to everything.
    self.response.set_status(204)

  def post(self):
    body = self.request.body.decode('utf-8')
    logging.info('Post body is %d characters', len(body))

    data = feedparser.parse(self.request.body)
    if data.bozo:
      logging.error('Bozo feed data. %s: %r',
                     data.bozo_exception.__class__.__name__,
                     data.bozo_exception)
      if (hasattr(data.bozo_exception, 'getLineNumber') and
          hasattr(data.bozo_exception, 'getMessage')):
        line = data.bozo_exception.getLineNumber()
        logging.error('Line %d: %s', line, data.bozo_exception.getMessage())
        segment = self.request.body.split('\n')[line-1]
        logging.info('Body segment with error: %r', segment.decode('utf-8'))
      return self.response.set_status(500)

    source = 'Source not supplied'
    if hasattr(data.feed, 'links'):
      for link in data.feed.links:
        if link.rel == 'self' and 'atom' in link.type:
          source = link.href
          break
    if not source:
      logging.error('Could not find feed source link: %s', data.links)

    update_list = []
    for entry in data.entries:
      update_list.append(SomeUpdate(
          key_name='key_' + hashlib.sha1(source + '\n' + entry.id).hexdigest(),
          title=entry.title,
          content=entry.content[0].value,
          updated=entry.updated,
          source=source))
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


class ViewHandler(webapp.RequestHandler):
  """Shows the items to anyone as HTML."""

  def get(self):
    context = dict(entries=SomeUpdate.gql('ORDER BY updated DESC').fetch(50))
    self.response.out.write(template.render('subscriber.html', context))


class ItemsHandler(webapp.RequestHandler):
  """Gets the items."""

  def get(self):
    encoder = simplejson.JSONEncoder()
    stuff = []
    for update in SomeUpdate.gql('ORDER BY updated DESC').fetch(50):
      stuff.append({'time': update.updated,
                    'title': update.title,
                    'content': update.content,
                    'source': update.source})
    self.response.out.write(encoder.encode(stuff))


application = webapp.WSGIApplication(
  [
    (r'/items', ItemsHandler),
    (r'/debug', DebugHandler),
    # Wildcard below so we can test multiple subscribers in a single app.
    (r'/subscriber.*', InputHandler),
    (r'/', ViewHandler),
  ],
  debug=True)


def main():
  wsgiref.handlers.CGIHandler().run(application)


if __name__ == '__main__':
  main()
