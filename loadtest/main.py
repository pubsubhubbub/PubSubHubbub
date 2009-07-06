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

"""Loadtest script that provides a dummy publisher and subscriber.

Every time a feed is requested from this loadtest publisher, it will produce a
new, random Atom payload. The loadtest app also provides a subscriber interface
that will accept any subscription. All notification messages will be thrown
away; it only keeps a count.
"""

import random
import wsgiref.handlers
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template


class FeedHandler(webapp.RequestHandler):

  def get(self, name):
    self.response.headers['Content-Type'] = 'application/xml+atom'
    self.response.out.write(template.render('atom.xml', {
      'self_url': self.request.url,
      'all_ids': random.sample(xrange(10**9), 25),
    }))


class SubscriberHandler(webapp.RequestHandler):

  def get(self, name):
    self.response.out.write(self.request.get('hub.challenge'))
    self.response.set_status(200)

  def post(self, name):
    pass


application = webapp.WSGIApplication([
  (r'/feed/(.*)', FeedHandler),
  (r'/subscriber/(.*)', SubscriberHandler),
], debug=True)


def main():
  wsgiref.handlers.CGIHandler().run(application)


if __name__ == '__main__':
  main()
