#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
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
import urllib
import wsgiref.handlers
from google.appengine.api import urlfetch
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext import db


HUB = 'http://localhost:8083/publish'


class Message(db.Model):
  title = db.TextProperty(required=True)
  content = db.TextProperty(required=True)
  when = db.DateTimeProperty(auto_now_add=True)

  def get_zulu_time(self):
    return self.when.strftime("%Y-%m-%d %H:%M:%SZ")


class MainHandler(webapp.RequestHandler):
  def get(self):
    self.response.out.write(template.render('input.html', {}))

  def post(self):
    message = Message(title=self.request.get('title'),
                      content=self.request.get('content'))
    message.put()

    headers = {'content-type': 'application/x-www-form-urlencoded'}
    post_params = {
      'hub.mode': 'publish',
      'hub.url': self.request.host_url + '/feed',
    }
    payload = urllib.urlencode(post_params)
    response = urlfetch.fetch(HUB, method='POST', payload=payload)
    logging.info('URL fetch status_code=%d, content="%s"',
                 response.status_code, response.content)

    self.redirect('/')


class FeedHandler(webapp.RequestHandler):
  def get(self):
    messages = Message.gql('ORDER BY when DESC').fetch(20)
    context = {
      'messages': messages
    }
    if messages:
      context['first_message'] = messages[0]
    self.response.headers['content-type'] = 'application/xml'
    self.response.out.write(template.render('atom.xml', context))


def main():
  application = webapp.WSGIApplication([('/', MainHandler),
                                        ('/feed', FeedHandler)],
                                       debug=True)
  wsgiref.handlers.CGIHandler().run(application)


if __name__ == '__main__':
  main()
