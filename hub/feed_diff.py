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

"""Atom feed parser that quickly filters enties by update time."""

import logging
import xml.sax
import xml.sax.handler


# Set to true to see stack level messages and other debugging information.
DEBUG = False


class FeedContentHandler(xml.sax.handler.ContentHandler):
  """Sax content handler for quickly parsing Atom feeds."""

  def __init__(self, updated_cutoff):
    self.updated_cutoff = updated_cutoff
    self.header_footer = ""
    self.entries_map = {}

    # Internal state
    self.stack_level = 0
    self.output_stack = []
    self.current_level = None
    self.last_id = ''
    self.last_updated = ''

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
      self.emit([' ', key, '="', value, '"'])
    self.emit('>')

    self.push()

  def endElement(self, name):  
    event = (self.stack_level, name)
    if DEBUG: logging.debug('End stack level %r', event)

    content = self.pop()
    self.emit(content)
    self.emit(['</', name, '>'])

    if event == (1, 'feed'):
      self.header_footer = ''.join(self.pop())
    elif event == (2, 'entry'):
      if self.last_updated < self.updated_cutoff:
        if DEBUG: logging.debug('Not saving content due to update cutoff')
        self.pop()
      else:
        self.entries_map[self.last_id] = (self.last_updated,
                                          ''.join(self.pop()))
    elif event == (3, 'updated'):
      self.last_updated = ''.join(content).strip()
      self.emit(self.pop())
    elif event == (3, 'id'):
      self.last_id = ''.join(content).strip()
      self.emit(self.pop())
    else:
      self.emit(self.pop())
    
    self.stack_level -= 1

  def characters(self, content):
    self.emit(content)


def filter(updated_cutoff, data):
  """Filter a feed by an update cutoff time.
  
  Args:
    updated_cutoff: Cutoff time as a string containing an ISO 8601 datetime.
    data: String containing the data of the XML feed to parse. For now, this
      must be an Atom feed.
  
  Returns:
    Tuple (header_footer, entries_map) where:
      header_footer: String containing everything else in the feed document
        that is specifically *not* an <entry>.
      entries_map: Dictionary mapping entry_id to tuple (updated, content)
        where updated is a string with the ISO 8601 timestamp of when the entry
        was updated, and content is a string containing the entry XML data.
  """
  handler = FeedContentHandler(updated_cutoff)
  xml.sax.parseString(data, handler)
  return handler.header_footer, handler.entries_map


__all__ = ['filter', 'DEBUG']
