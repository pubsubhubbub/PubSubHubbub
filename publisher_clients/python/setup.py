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

from distutils.core import setup


LONG_DESC = (
    'A simple, open, server-to-server web-hook-based pubsub '
    '(publish/subscribe) protocol as a simple extension to Atom. '
    'Parties (servers) speaking the PubSubHubbub protocol can get '
    'near-instant notifications (via webhook callbacks) when a topic '
    '(Atom URL) they\'re interested in is updated.')

setup(name='PubSubHubbub_Publisher',
      version='1.0',
      description='Publisher client for PubSubHubbub',
      long_description=LONG_DESC,
      author='Brett Slatkin',
      author_email='bslatkin@gmail.com',
      url='http://code.google.com/p/pubsubhubbub/',
      py_modules=['pubsubhubbub_publish'],
      license="Apache 2.0")
