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

__author__ = 'bslatkin@gmail.com (Brett Slatkin)'

"""Atom/RSS feed parser that determines a feed's canonical ID."""

import cStringIO
import logging
import re
import xml.sax
import xml.sax.handler
import xml.sax.saxutils


# Set to true to see stack level messages and other debugging information.
DEBUG = False


class TrivialEntityResolver(xml.sax.handler.EntityResolver):
  """Pass-through entity resolver."""

  def resolveEntity(self, publicId, systemId):
    return cStringIO.StringIO()


class FeedIdentifier(xml.sax.handler.ContentHandler):
  """Base SAX content handler for identifying feeds."""

  target_tag_stack = None

  def __init__(self, parser):
    """Initializer.

    Args:
      parser: Instance of the xml.sax parser being used with this handler.
    """
    self.parser = parser
    self.link = []
    self.tag_stack = []
    self.capture_next_element = False

  # SAX methods
  def startElement(self, name, attrs):
    if not self.link:
      if DEBUG: logging.debug('Start stack level for %r', name)
      self.tag_stack.append(name)
      if len(self.tag_stack) == len(self.target_tag_stack):
        equal = True
        for value, predicate in zip(self.tag_stack, self.target_tag_stack):
          if not predicate(value):
            equal = False
            break
        if equal:
          self.capture_next_element = True

  def endElement(self, name):
    if self.link:
      self.capture_next_element = False
    else:
      if DEBUG: logging.debug('End stack level %r', name)
      self.tag_stack.pop()

  def characters(self, content):
    if self.capture_next_element:
      self.link.append(content)

  def get_link(self):
    if not self.link:
      return None
    else:
      return ''.join(self.link).strip()


class AtomFeedIdentifier(FeedIdentifier):
  """SAX content handler for identifying Atom feeds."""

  target_tag_stack = [
      re.compile(k).match for k in (
      '([^:]+:)?feed$',
      '([^:]+:)?id$')]


class RssFeedIdentifier(FeedIdentifier):
  """SAX content handler for identifying RSS feeds."""

  target_tag_stack = (
      [re.compile('^(?i)(rss)|(.*rdf)$').match] +
      [re.compile(k).match for k in ('channel', 'link')])


def identify(data, format):
  """Identifies a feed.

  Args:
    data: String containing the data of the XML feed to parse.
    format: String naming the format of the data. Should be 'rss' or 'atom'.

  Returns:
    The ID of the feed, or None if one could not be determined (due to parse
    errors, etc).

  Raises:
    xml.sax.SAXException on parse errors.
  """
  data_stream = cStringIO.StringIO(data)
  parser = xml.sax.make_parser()

  if format == 'atom':
    handler = AtomFeedIdentifier(parser)
  elif format == 'rss':
    handler = RssFeedIdentifier(parser)
  else:
    assert False, 'Invalid feed format "%s"' % format

  parser.setContentHandler(handler)
  parser.setEntityResolver(TrivialEntityResolver())
  parser.parse(data_stream)

  return handler.get_link()


__all__ = ['identify', 'DEBUG']
