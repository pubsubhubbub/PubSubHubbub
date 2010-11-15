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

"""Tests for the feed_diff module."""

import logging
import os
import unittest

import feed_diff


class TestBase(unittest.TestCase):

  format = None
  feed_open = None
  feed_close = None
  entry_open = None
  entry_close = None

  def setUp(self):
    self.testdata = os.path.join(os.path.dirname(__file__),
                                 'feed_diff_testdata')

  def verify_entries(self, expected_list, entries):
    found_entries = sorted(entries.items())
    self.assertEqual(len(expected_list), len(found_entries))
    for index, (expected_key, found) in enumerate(
        zip(expected_list, found_entries)):
      found_key, found_content = found
      self.assertEqual(expected_key, found_key,
          "Fail on index %d: Expected %r, found %r" % (
          index, expected_key, found_key))
      self.assertTrue(found_content.startswith(self.entry_open))
      self.assertTrue(found_content.endswith(self.entry_close))

  def load_feed(self, path):
    data = open(os.path.join(self.testdata, path)).read()
    header_footer, entries = feed_diff.filter(data, self.format)
    self.assertTrue(header_footer.startswith(self.feed_open))
    self.assertTrue(header_footer.endswith(self.feed_close))
    return header_footer, entries


class AtomFeedDiffTest(TestBase):

  format = 'atom'
  feed_open = '<feed'
  feed_close = '</feed>'
  entry_open = '<entry>'
  entry_close = '</entry>'

  def testParsing(self):
    """Tests parsing."""
    header_footer, entries = self.load_feed('parsing.xml')
    expected_list = [
        u'tag:diveintomark.org,2008-06-29:/archives/20080629044756',
        u'tag:diveintomark.org,2008-07-04:/archives/20080704050619',
        u'tag:diveintomark.org,2008-07-06:/archives/20080706022239',
        u'tag:diveintomark.org,2008-07-12:/archives/20080712042845',
        u'tag:diveintomark.org,2008-07-13:/archives/20080713011654',
        u'tag:diveintomark.org,2008-07-17:/archives/20080717044506',
        u'tag:diveintomark.org,2008-07-23:/archives/20080723030709',
        u'tag:diveintomark.org,2008-07-29:/archives/20080729021401',
        u'tag:diveintomark.org,2008-08-05:/archives/20080805020410',
        u'tag:diveintomark.org,2008-08-05:/archives/20080805155619',
        u'tag:diveintomark.org,2008-08-06:/archives/20080806144009',
        u'tag:diveintomark.org,2008-08-07:/archives/20080807025755',
        u'tag:diveintomark.org,2008-08-07:/archives/20080807233337',
        u'tag:diveintomark.org,2008-08-12:/archives/20080812160843',
        u'tag:diveintomark.org,2008-08-14:/archives/20080814215936',
    ]
    self.verify_entries(expected_list, entries)
    # Verify whitespace cleanup.
    self.assertTrue(header_footer.endswith('>\n</feed>'))
    # Verify preservation of '/>' closings.
    self.assertTrue('<link href="http://diveintomark.org/" '
                    'type="text/html" rel="alternate"/>' in header_footer)

  def testEntityEscaping(self):
    """Tests when certain external entities show up in the feed.

    Example: '&amp;nbsp' will be converted to '&nbsp;' by the parser, but then
    the new output entity won't be resolved.
    """
    header_footer, entries = self.load_feed('entity_escaping.xml')
    self.assertTrue('&#x27;' not in header_footer)
    entity_id, content = entries.items()[0]
    self.assertTrue('&amp;nbsp;' in content)

  def testAttributeEscaping(self):
    """Tests when certain external entities show up in an XML attribute.

    Example: gd:foo="&quot;blah&quot;" will be converted to
    gd:foo=""blah"" by the parser, which is not valid XML when reconstructing
    the result.
    """
    header_footer, entries = self.load_feed('attribute_escaping.xml')
    self.assertTrue('foo:myattribute="&quot;\'foobar\'&quot;"' in header_footer)

  def testInvalidFeed(self):
    """Tests when the feed is not a valid Atom document."""
    data = open(os.path.join(self.testdata, 'bad_atom_feed.xml')).read()
    try:
      feed_diff.filter(data, 'atom')
    except feed_diff.Error, e:
      self.assertTrue('Enclosing tag is not <feed></feed>' in str(e))
    else:
      self.fail()

  def testNoXmlHeader(self):
    """Tests that feeds with no XML header are accepted."""
    data = open(os.path.join(self.testdata, 'no_xml_header.xml')).read()
    header_footer, entries = feed_diff.filter(data, 'atom')
    self.assertEquals(1, len(entries))

  def testMissingId(self):
    """Tests when an Atom entry is missing its ID field."""
    data = open(os.path.join(self.testdata, 'missing_entry_id.xml')).read()
    try:
      feed_diff.filter(data, 'atom')
    except feed_diff.Error, e:
      self.assertTrue('<entry> element missing <id>' in str(e))
    else:
      self.fail()

  def testFailsOnRss(self):
    """Tests that parsing an RSS feed as Atom will fail."""
    data = open(os.path.join(self.testdata, 'rss2sample.xml')).read()
    try:
      feed_diff.filter(data, 'atom')
    except feed_diff.Error, e:
      self.assertTrue('Enclosing tag is not <feed></feed>' in str(e))
    else:
      self.fail()

  def testCData(self):
    """Tests a feed that has a CData section."""
    data = open(os.path.join(self.testdata, 'cdata_test.xml')).read()
    header_footer, entries = feed_diff.filter(data, 'atom')
    expected_list = [
        u'tag:blog.livedoor.jp,2010:coupon_123.1635380'
    ]
    self.verify_entries(expected_list, entries)
    self.assertTrue(
        ('<generator url="http://blog.livedoor.com/" '
         'version="1.0">livedoor Blog</generator>')
        in header_footer)
    entry_data = entries['tag:blog.livedoor.jp,2010:coupon_123.1635380']
    # Here the CData section is rewritten.
    self.assertTrue('&lt;/FONT&gt;' in entry_data)


class AtomNamespacedFeedDiffTest(TestBase):

  format = 'atom'
  feed_open = '<atom:feed'
  feed_close = '</atom:feed>'
  entry_open = '<atom:entry>'
  entry_close = '</atom:entry>'

  def testFullNamespacing(self):
    """Tests when the whole XML payload uses atom: namespace prefixes."""
    header_footer, entries = self.load_feed('atom_namespace.xml')
    self.assertTrue(header_footer.endswith('</atom:feed>'))
    expected_list = [
      u'http://example.com/feeds/delta/124',
      u'http://example.com/feeds/delta/125'
    ]
    self.verify_entries(expected_list, entries)


class RssFeedDiffTest(TestBase):

  format = 'rss'
  feed_open = '<rss'
  feed_close = '</rss>'
  entry_open = '<item>'
  entry_close = '</item>'

  def testParsingRss20(self):
    """Tests parsing RSS 2.0."""
    header_footer, entries = self.load_feed('rss2sample.xml')
    expected_list = [
        u'http://liftoff.msfc.nasa.gov/2003/05/20.html#item570',
        u'http://liftoff.msfc.nasa.gov/2003/05/27.html#item571',
        u'http://liftoff.msfc.nasa.gov/2003/05/30.html#item572',
        u'http://liftoff.msfc.nasa.gov/2003/06/03.html#item573',
    ]
    self.verify_entries(expected_list, entries)
    # Verify whitespace cleanup.
    self.assertTrue(header_footer.endswith('>\n</channel>\n</rss>'))
    # Verify preservation of '/>' closings.
    self.assertTrue('<mycoolelement wooh="fun"/>' in header_footer)

  def testParsingRss091(self):
    """Tests parsing RSS 0.91."""
    header_footer, entries = self.load_feed('sampleRss091.xml')
    expected_list = [
        u'http://writetheweb.com/read.php?item=19',
        u'http://writetheweb.com/read.php?item=20',
        u'http://writetheweb.com/read.php?item=21',
        u'http://writetheweb.com/read.php?item=22',
        u'http://writetheweb.com/read.php?item=23',
        u'http://writetheweb.com/read.php?item=24',
    ]
    self.verify_entries(expected_list, entries)

  def testParsingRss092(self):
    """Tests parsing RSS 0.92 with enclosures and only descriptions."""
    header_footer, entries = self.load_feed('sampleRss092.xml')
    expected_list = [
        u'&lt;a href="http://arts.ucsc.edu/GDead/AGDL/other1.html"&gt;The Other One&lt;/a&gt;, live instrumental, One From The Vault. Very rhythmic very spacy, you can listen to it many times, and enjoy something new every time.',
        u'&lt;a href="http://www.cs.cmu.edu/~mleone/gdead/dead-lyrics/Franklin\'s_Tower.txt"&gt;Franklin\'s Tower&lt;/a&gt;, a live version from One From The Vault.',
        u'&lt;a href="http://www.scripting.com/mp3s/youWinAgain.mp3"&gt;The news is out&lt;/a&gt;, all over town..&lt;p&gt;\nYou\'ve been seen, out runnin round. &lt;p&gt;\nThe lyrics are &lt;a href="http://www.cs.cmu.edu/~mleone/gdead/dead-lyrics/You_Win_Again.txt"&gt;here&lt;/a&gt;, short and sweet. &lt;p&gt;\n&lt;i&gt;You win again!&lt;/i&gt;',
        u"It's been a few days since I added a song to the Grateful Dead channel. Now that there are all these new Radio users, many of whom are tuned into this channel (it's #16 on the hotlist of upstreaming Radio users, there's no way of knowing how many non-upstreaming users are subscribing, have to do something about this..). Anyway, tonight's song is a live version of Weather Report Suite from Dick's Picks Volume 7. It's wistful music. Of course a beautiful song, oft-quoted here on Scripting News. &lt;i&gt;A little change, the wind and rain.&lt;/i&gt;",
        u'Kevin Drennan started a &lt;a href="http://deadend.editthispage.com/"&gt;Grateful Dead Weblog&lt;/a&gt;. Hey it\'s cool, he even has a &lt;a href="http://deadend.editthispage.com/directory/61"&gt;directory&lt;/a&gt;. &lt;i&gt;A Frontier 7 feature.&lt;/i&gt;',
        u'Moshe Weitzman says Shakedown Street is what I\'m lookin for for tonight. I\'m listening right now. It\'s one of my favorites. "Don\'t tell me this town ain\'t got no heart." Too bright. I like the jazziness of Weather Report Suite. Dreamy and soft. How about The Other One? "Spanish lady come to me.."',
        u'The HTML rendering almost &lt;a href="http://validator.w3.org/check/referer"&gt;validates&lt;/a&gt;. Close. Hey I wonder if anyone has ever published a style guide for ALT attributes on images? What are you supposed to say in the ALT attribute? I sure don\'t know. If you\'re blind send me an email if u cn rd ths.',
        u'This is a test of a change I just made. Still diggin..',
    ]
    self.verify_entries(expected_list, entries)

  def testOnlyLink(self):
    """Tests when an RSS item only has a link element."""
    header_footer, entries = self.load_feed('rss2_only_link.xml')
    expected_list = [
        u'http://liftoff.msfc.nasa.gov/news/2003/news-VASIMR.asp',
        u'http://liftoff.msfc.nasa.gov/news/2003/news-laundry.asp',
        u'http://liftoff.msfc.nasa.gov/news/2003/news-starcity.asp',
    ]
    self.verify_entries(expected_list, entries)

  def testOnlyTitle(self):
    """Tests when an RSS item only has a title element."""
    header_footer, entries = self.load_feed('rss2_only_title.xml')
    expected_list = [
        u"Astronauts' Dirty Laundry",
        u'Star City',
        u'The Engine That Does More',
    ]
    self.verify_entries(expected_list, entries)

  def testFailsOnAtom(self):
    """Tests that parsing an Atom feed as RSS will fail."""
    data = open(os.path.join(self.testdata, 'parsing.xml')).read()
    try:
      feed_diff.filter(data, 'rss')
    except feed_diff.Error, e:
      self.assertTrue('Enclosing tag is not <rss></rss>' in str(e))
    else:
      self.fail()


class RssRdfFeedDiffTest(TestBase):

  format = 'rss'
  feed_open = '<rdf:RDF'
  feed_close = '</rdf:RDF>'
  entry_open = '<item'
  entry_close = '</item>'

  def testParsingRss10Rdf(self):
    """Tests parsing RSS 1.0, which is actually an RDF document."""
    header_footer, entries = self.load_feed('rss_rdf.xml')
    expected_list = [
        u'http://writetheweb.com/read.php?item=23',
        u'http://writetheweb.com/read.php?item=24',
    ]
    self.verify_entries(expected_list, entries)

  def testItemsOutsideChannel(self):
    """Tests when the RDF items are listed outside the channel element."""
    header_footer, entries = self.load_feed('rdf_10_weirdness.xml')
    expected_list = [
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/-gWGIAF-QGY/Iridium-Pushes-Ahead-Satellite-Project', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/D7AUWrUlSys/Mobile-Phones-vs-Supercomputers-of-the-Past', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/Fyn4QmQHquA/Doctor-Slams-Hospitals-Please-Policy', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/JcnvobAj0DE/Mars500-Mission-Begins', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/OsEit_oEJG8/Police-Officers-Seek-Right-Not-To-Be-Recorded', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/RWSAy1nVfYQ/FTC-Staff-Discuss-a-Tax-on-Electronics-To-Support-the-News-Business', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/WQA5Xo_6cwA/Military-Develops-Green-Cleaners-For-Terrorist-Attack-Sites', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/cshWw102qTE/Frank-Zappas-Influence-On-Linux-and-FOSS-Development', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/iW74en1N7Ro/Bill-Gives-Feds-Emergency-Powers-To-Secure-Civilian-Nets', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/lAWnaU6YgIs/Why-Are-Indian-Kids-So-Good-At-Spelling', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/qALRtOq3asc/OH-Senate-Passes-Bill-Banning-Human-Animal-Hybrids', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/sH47rA1Mhcs/Part-Human-Part-Machine-Transistor-Devised', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/tmmQTFyzGb8/How-To-Get-Rejected-From-the-App-Store', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/wYu2IZPqdzM/Yahoo-Treading-Carefully-Before-Exposing-More-Private-Data', 
      u'http://rss.slashdot.org/~r/Slashdot/slashdot/~3/ziiB2Fy7vGY/Six-Major-3G-and-4G-Networks-Tested-Nationwide'
    ]
    self.assertTrue(header_footer.endswith('</rdf:RDF>'))
    self.verify_entries(expected_list, entries)


class FilterTest(TestBase):

  format = 'atom'

  def testEntities(self):
    """Tests that external entities cause parsing to fail."""
    try:
      self.load_feed('xhtml_entities.xml')
      self.fail('Should have raised an exception')
    except feed_diff.Error, e:
      # TODO(bslatkin): Fix this datafile in head.
      # This ensures that hte failure is because of bad entities, not a
      # missing test data file.
      self.assertFalse('IOError' in str(e))


if __name__ == '__main__':
  ## feed_diff.DEBUG = True
  ## logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
