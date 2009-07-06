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

__author__ = 'bslatkin@gmail.com (Brett Slatkin)'

"""Atom/RSS feed parser that quickly extracts entry/item elements."""

import cStringIO
import logging
import xml.sax
import xml.sax.handler
import xml.sax.saxutils


# Set to true to see stack level messages and other debugging information.
DEBUG = False


class Error(Exception):
  """Exception for errors in this module."""


class FeedContentHandler(xml.sax.handler.ContentHandler):
  """Sax content handler for quickly parsing Atom and RSS feeds."""

  def __init__(self, parser):
    """Initializer.

    Args:
      parser: Instance of the xml.sax parser being used with this handler.
    """
    self.parser = parser
    self.header_footer = ""
    self.entries_map = {}

    # Internal state
    self.stack_level = 0
    self.output_stack = []
    self.current_level = None
    self.last_id = ''
    self.last_link = ''
    self.last_title = ''
    self.last_description = ''

  # Helper methods
  def emit(self, data):
    if type(data) is list:
      self.current_level.extend(data)
    else:
      self.current_level.append(data)

  def push(self):
    self.current_level = []
    self.output_stack.append(self.current_level)

  def pop(self):
    old_level = self.output_stack.pop()
    if len(self.output_stack) > 0:
      self.current_level = self.output_stack[-1]
    else:
      self.current_level = None
    return old_level

  # SAX methods
  def startElement(self, name, attrs):
    self.stack_level += 1
    event = (self.stack_level, name)
    if DEBUG: logging.debug('Start stack level %r', event)

    self.push()
    self.emit(['<', name])
    for key, value in attrs.items():
      self.emit([' ', key, '=', xml.sax.saxutils.quoteattr(value)])
    # Do not emit a '>' here because this tag may need to be immediately
    # closed with a '/> ending.

    self.push()

  def endElement(self, name):
    event = (self.stack_level, name)
    if DEBUG: logging.debug('End stack level %r', event)

    content = self.pop()
    if content:
      self.emit('>')
      self.emit(content)
      self.emit(['</', name, '>'])
    else:
      # No content means this element should be immediately closed.
      self.emit('/>')

    self.handleEvent(event, content)
    self.stack_level -= 1

  def characters(self, content):
    # The SAX parser will try to escape XML entities (like &amp;) and other
    # fun stuff. But this is not what we actually want. We want the original
    # content to be reproduced exactly as we received it, so we can pass it
    # along to others. The reason is simple: reformatting the XML by unescaping
    # certain data may cause the resulting XML to no longer validate.
    self.emit(xml.sax.saxutils.escape(content))


def strip_whitespace(type, all_parts):
  """Strips the whitespace from a SAX parser list for a feed.

  Args:
    all_parts: List of SAX parser elements.

  Returns:
    header_footer for those parts with trailing whitespace removed.
  """
  if type == 'atom':
    first_part = ''.join(all_parts[:-3]).strip('\n\r\t ')
    return '%s\n</feed>' % first_part
  else:
    first_part = ''.join(all_parts[:-3]).strip('\n\r\t ')
    channel_part = first_part.rfind('</channel>')
    if channel_part == -1:
      raise Error('Could not find </channel> after trimming whitespace')
    stripped = first_part[:channel_part].strip('\n\r\t ')
    return '%s\n</channel>\n</rss>' % stripped


class AtomFeedHandler(FeedContentHandler):
  """Sax content handler for Atom feeds."""

  def handleEvent(self, event, content):
    if event[0] == 1:
      if event[1] != 'feed':
        raise Error('Enclosing tag is not <feed></feed>')
      else:
        self.header_footer = strip_whitespace('atom', self.pop())
    elif event == (2, 'entry'):
      self.entries_map[self.last_id] = ''.join(self.pop())
    elif event == (3, 'id'):
      self.last_id = ''.join(content).strip()
      self.emit(self.pop())
    else:
      self.emit(self.pop())


class RssFeedHandler(FeedContentHandler):
  """Sax content handler for RSS feeds."""

  def handleEvent(self, event, content):
    if event[0] == 1:
      if event[1] != 'rss':
        raise Error('Enclosing tag is not <rss></rss>')
      else:
        self.header_footer = strip_whitespace('rss', self.pop())
    elif event == (3, 'item'):
      item_id = (self.last_id or self.last_link or
                 self.last_title or self.last_description)
      self.entries_map[item_id] = ''.join(self.pop())
      self.last_id, self.last_link, self.last_title, self.last_description = (
          '', '', '', '')
    elif event == (4, 'guid'):
      self.last_id = ''.join(content).strip()
      self.emit(self.pop())
    elif event == (4, 'link'):
      self.last_link = ''.join(content).strip()
      self.emit(self.pop())
    elif event == (4, 'title'):
      self.last_title = ''.join(content).strip()
      self.emit(self.pop())
    elif event == (4, 'description'):
      self.last_description = ''.join(content).strip()
      self.emit(self.pop())
    else:
      self.emit(self.pop())


def filter(data, format):
  """Filter a feed through the parser.

  Args:
    data: String containing the data of the XML feed to parse.
    format: String naming the format of the data. Should be 'rss' or 'atom'.

  Returns:
    Tuple (header_footer, entries_map) where:
      header_footer: String containing everything else in the feed document
        that is specifically *not* an <entry> or <item>.
      entries_map: Dictionary mapping entry_id to the entry's XML data.

  Raises:
    xml.sax.SAXException on parse errors. feed_diff.Error if the diff could not
    be derived due to bad content (e.g., a good XML doc that is not Atom or RSS)
    or any of the feed entries are missing required fields.
  """
  data_stream = cStringIO.StringIO(data)
  parser = xml.sax.make_parser()

  if format == 'atom':
    handler = AtomFeedHandler(parser)
  elif format == 'rss':
    handler = RssFeedHandler(parser)
  else:
    raise Error('Invalid feed format "%s"' % format)

  parser.setContentHandler(handler)
  parser.parse(data_stream)

  for entry_id, content in handler.entries_map.iteritems():
    if format == 'atom' and not entry_id:
      raise Error('<entry> element missing <id>: %s' % content)
    elif format == 'rss' and not entry_id:
      raise Error('<item> element missing <guid> or <link>: %s' % content)

  return handler.header_footer, handler.entries_map


__all__ = ['filter', 'DEBUG', 'Error']
