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

"""PubSubHubbub protocol Hub implementation built on Google App Engine.

=== Model classes:

* Subscription: A single subscriber's lease on a topic URL. Also represents a
  work item of a subscription that is awaiting confirmation (sub. or unsub).

* FeedToFetch: Work item inserted when a publish event occurs. This will be
  moved to the Task Queue API once available.

* KnownFeed: Materialized view of all distinct topic URLs. Written blindly on
  successful subscriptions; may be out of date after unsubscription. Used for
  doing bootstrap polling of feeds that are not Hub aware.

* FeedRecord: Metadata information about a feed, the last time it was polled,
  and any headers that may affect future polling. Also contains any debugging
  information about the last feed fetch and why it may have failed.

* FeedEntryRecord: Record of a single entry in a single feed. May eventually
  be garbage collected after enough time has passed since it was last seen.

* EventToDeliver: Work item that contains the content to deliver for a feed
  event. Maintains current position in subscribers and number of delivery
  failures. Used to coordinate delivery retries. Will be deleted in successful
  cases or stick around in the event of complete failures for debugging.

* PollingMarker: Work item that keeps track of the last time all KnownFeed
  instances were fetched. Used to do bootstrap polling.


=== Entity groups:

Subscription entities are in their own entity group to allow for a high number
of simultaneous subscriptions for the same topic URL. FeedToFetch is also in
its own entity group for the same reason. FeedRecord, FeedEntryRecord, and
EventToDeliver entries are all in the same entity group, however, to ensure that
each feed polling is either full committed and delivered to subscribers or fails
and will be retried at a later time.

                  ------------
                 | FeedRecord |
                  -----+------
                       |
                       |
         +-------------+-------------+
         |                           |
         |                           |
 --------+--------           --------+-------
| FeedEntryRecord |         | EventToDeliver |
 -----------------           ----------------
"""

# Bigger TODOs (now in priority order)
#
# - Add Subscription delivery diagnostics, so subscribers can understand what
#   error the hub has been seeing when we try to deliver a feed to them.
#
# - Add subscription counting to PushEventHandler so we can deliver a header
#   with the number of subscribers the feed has. This will simply just keep
#   count of the subscribers seen so far and then when the pushing is done it
#   will save that total back on the FeedRecord instance.
#
# - Improve polling algorithm to keep stats on each feed.
#
# - Do not poll a feed if we've gotten an event from the publisher in less
#   than the polling period.
#
# - Add Subscription expiration cronjob to clean up expired subscriptions.
#
# - Add maximum subscription count per callback domain.
#

import datetime
import hashlib
import hmac
import logging
import os
import random
import sgmllib
import time
import urllib
import urlparse
import wsgiref.handlers
import xml.sax

from google.appengine import runtime
from google.appengine.api import datastore_types
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.api import urlfetch_errors
from google.appengine.api import users
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.runtime import apiproxy_errors

import async_apiproxy
import dos
import feed_diff
import urlfetch_async

async_proxy = async_apiproxy.AsyncAPIProxy()

################################################################################
# Config parameters

DEBUG = True

if DEBUG:
  logging.getLogger().setLevel(logging.DEBUG)

# How many subscribers to contact at a time when delivering events.
EVENT_SUBSCRIBER_CHUNK_SIZE = 10

# Maximum number of times to attempt a subscription retry.
MAX_SUBSCRIPTION_CONFIRM_FAILURES = 4

# Period to use for exponential backoff on subscription confirm retries.
SUBSCRIPTION_RETRY_PERIOD = 30 # seconds

# Maximum number of times to attempt to pull a feed.
MAX_FEED_PULL_FAILURES = 4

# Period to use for exponential backoff on feed pulling.
FEED_PULL_RETRY_PERIOD = 30 # seconds

# Maximum number of times to attempt to deliver a feed event.
MAX_DELIVERY_FAILURES = 4

# Period to use for exponential backoff on feed event delivery.
DELIVERY_RETRY_PERIOD = 30 # seconds

# Number of polling feeds to fetch from the Datastore at a time.
BOOSTRAP_FEED_CHUNK_SIZE = 200

# How often to poll feeds.
POLLING_BOOTSTRAP_PERIOD = 10800  # in seconds; 3 hours

# Default expiration time of a lease.
DEFAULT_LEASE_SECONDS = (30 * 24 * 60 * 60)  # 30 days

# Maximum expiration time of a lease.
MAX_LEASE_SECONDS = DEFAULT_LEASE_SECONDS * 3  # 90 days

################################################################################
# Constants

ATOM = 'atom'
RSS = 'rss'

VALID_PORTS = frozenset([
    '80', '443', '4443', '8080', '8081', '8082', '8083', '8084', '8085',
    '8086', '8087', '8088', '8089', '8188', '8444', '8990'])

EVENT_QUEUE = 'event-delivery'

FEED_QUEUE = 'feed-pulls'

POLLING_QUEUE = 'polling'

SUBSCRIPTION_QUEUE = 'subscriptions'

WEBLOGS_XMLRPC_ERROR = """<?xml version="1.0"?>
<methodResponse><params><param><value><struct>
<member><name>flerror</name><value><boolean>1</boolean></value></member>
<member><name>message</name><value>%s</value></member>
<member><name>legal</name><value>
You agree that use of this ping service is governed
by the Terms of Use found at pubsubhubbub.appspot.com.
</value></member>
</struct></value></param></params></methodResponse>
"""

WEBLOGS_XMLRPC_SUCCESS = """<?xml version="1.0"?>
<methodResponse><params><param><value><struct>
<member><name>flerror</name><value><boolean>0</boolean></value></member>
<member><name>message</name><value>Thanks for the ping.</value></member>
<member><name>legal</name><value>
You agree that use of this ping service is governed
by the Terms of Use found at pubsubhubbub.appspot.com.
</value></member>
</struct></value></param></params></methodResponse>
"""

################################################################################
# Helper functions

def sha1_hash(value):
  """Returns the sha1 hash of the supplied value."""
  return hashlib.sha1(value.encode('utf-8')).hexdigest()


def get_hash_key_name(value):
  """Returns a valid entity key_name that's a hash of the supplied value."""
  return 'hash_' + sha1_hash(value)


def sha1_hmac(secret, data):
  """Returns the sha1 hmac for a chunk of data and a secret."""
  return hmac.new(secret, data, hashlib.sha1).hexdigest()


def is_dev_env():
  """Returns True if we're running in the development environment."""
  return 'Dev' in os.environ.get('SERVER_SOFTWARE', '')


def work_queue_only(func):
  """Decorator that only allows a request if from cron job, task, or an admin.

  Also allows access if running in development server environment.

  Args:
    func: A webapp.RequestHandler method.

  Returns:
    Function that will return a 401 error if not from an authorized source.
  """
  def decorated(myself, *args, **kwargs):
    if ('X-AppEngine-Cron' in myself.request.headers or
        'X-AppEngine-TaskName' in myself.request.headers or
        is_dev_env() or users.is_current_user_admin()):
      return func(myself, *args, **kwargs)
    elif users.get_current_user() is None:
      myself.redirect(users.create_login_url(myself.request.url))
    else:
      myself.response.set_status(401)
      myself.response.out.write('Handler only accessible for work queues')
  return decorated


def is_valid_url(url):
  """Returns True if the URL is valid, False otherwise."""
  split = urlparse.urlparse(url)
  if not split.scheme in ('http', 'https'):
    logging.debug('URL scheme is invalid: %s', url)
    return False

  netloc, port = (split.netloc.split(':', 1) + [''])[:2]
  if port and not is_dev_env() and port not in VALID_PORTS:
    logging.debug('URL port is invalid: %s', url)
    return False

  if split.fragment:
    logging.debug('URL includes fragment: %s', url)
    return False

  return True


_VALID_CHARS = (
  'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
  'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
  'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
  '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_',
)


def get_random_challenge():
  """Returns a string containing a random challenge token."""
  return ''.join(random.choice(_VALID_CHARS) for i in xrange(128))


class HtmlDiscoveryParser(sgmllib.SGMLParser):
  """HTML parser that auto-discovers feed URLs.

  Based off of Mark Pilgrim's auto-discovery script from:
    http://diveintomark.org/archives/2002/05/31/rss_autodiscovery_in_python

  Thus, this class is roughly Copyright 2002, Mark Pilgrim and is
  under the Python license:
    http://www.python.org/psf/license/

  Feed URLs will be placed in the 'feed_urls' attribute's list.
  """

  def reset(self):
    sgmllib.SGMLParser.reset(self)
    self.feed_urls = []

  def end_head(self, attrs):
    self.setnomoretags()

  def start_body(self, attrs):
    self.setnomoretags()

  def do_link(self, attrs):
    attr_dict = dict(attrs)
    if attr_dict.get('rel').lower() != 'alternate':
      return
    type = attr_dict.get('type')
    if type not in ('application/atom+xml', 'application/rss+xml'):
      return
    href = attr_dict.get('href')
    # This URL may be bad, but it will be validated later.
    self.feed_urls.append(href)


class AutoDiscoveryError(Exception):
  """Raised when auto-discovery fails for whatever reason.

  The exception detail should be set to a descriptive string that could
  be presented to the requestor on the other side.
  """


def auto_discover_urls(blog_url):
  """Auto-discovers the feed links for a URL.

  Caches the discovered URLs in memcache.

  Args:
    blog_url: The feed to do auto-discovery on. May be a feed URL itself, in
      which case this URL will be returned.

  Returns:
    A list of feed URLs. May be multiple in cases where multiple formats or
    variants of a feed are auto-discovered.

  Raises:
    AutoDiscoveryError if auto-discovery fails for any reason.
  """
  key = 'auto_discover:' + blog_url
  mapping = memcache.get(key)
  if mapping:
    feed_urls = mapping.split('\n')
    logging.debug('Cache hit for auto-discovery of blog_url=%s: %s',
                  blog_url, feed_urls)
    return feed_urls

  try:
    result = urlfetch.fetch(blog_url)
  except (apiproxy_errors.Error, urlfetch.Error), e:
    logging.exception('Error fetching for discovery blog URL=%s', blog_url)
    raise AutoDiscoveryError('Error fetching content for auto-discovery')

  if result.status_code != 200:
    logging.error('Discovery status_code=%s for blog URL=%s',
                  result.status_code, blog_url)
    raise AutoDiscoveryError('Auto-discovery fetch received status code %s' %
                             result.status_code)

  content_type = result.headers.get('content-type', '')
  if 'xml' in content_type:
    # The supplied URL is actually XML, which means it *should* be a feed.
    feed_urls = [blog_url]
  elif 'html' in content_type:
    parser = HtmlDiscoveryParser()
    try:
      parser.feed(result.content)
    except sgmllib.SGMLParseError:
      logging.exception('Parsing HTML for auto-discovery '
                        'failed for blog URL=%s', blog_url)
      # Cache the error to prevent further, crappy load.
      memcache.add(key, '')
      raise AutoDiscoveryError('Could not parse HTML for auto-discovery')
    else:
      feed_urls = parser.feed_urls
  else:
    raise AutoDiscoveryError(
        'Blog URL has bad content-type for auto-discovery: %s' % content_type)

  memcache.add(key, '\n'.join(feed_urls))
  return feed_urls

################################################################################
# Models

class Subscription(db.Model):
  """Represents a single subscription to a topic for a callback URL."""

  STATE_NOT_VERIFIED = 'not_verified'
  STATE_VERIFIED = 'verified'
  STATE_TO_DELETE = 'to_delete'
  STATES = frozenset([
    STATE_NOT_VERIFIED,
    STATE_VERIFIED,
    STATE_TO_DELETE,
  ])

  callback = db.TextProperty(required=True)
  callback_hash = db.StringProperty(required=True)
  topic = db.TextProperty(required=True)
  topic_hash = db.StringProperty(required=True)
  created_time = db.DateTimeProperty(auto_now_add=True)
  last_modified = db.DateTimeProperty(auto_now=True)
  lease_seconds = db.IntegerProperty(default=DEFAULT_LEASE_SECONDS)
  expiration_time = db.DateTimeProperty(required=True)
  eta = db.DateTimeProperty(auto_now_add=True)
  confirm_failures = db.IntegerProperty(default=0)
  verify_token = db.TextProperty()
  secret = db.TextProperty()
  hmac_algorithm = db.TextProperty()
  subscription_state = db.StringProperty(default=STATE_NOT_VERIFIED,
                                         choices=STATES)

  @staticmethod
  def create_key_name(callback, topic):
    """Returns the key name for a Subscription entity.

    Args:
      callback: URL of the callback subscriber.
      topic: URL of the topic being subscribed to.

    Returns:
      String containing the key name for the corresponding Subscription.
    """
    return get_hash_key_name('%s\n%s' % (callback, topic))

  @classmethod
  def insert(cls,
             callback,
             topic,
             verify_token,
             secret,
             hash_func='sha1',
             lease_seconds=DEFAULT_LEASE_SECONDS,
             now=datetime.datetime.now):
    """Marks a callback URL as being subscribed to a topic.

    Creates a new subscription if None already exists. Forces any existing,
    pending request (i.e., async) to immediately enter the verified state.

    Args:
      callback: URL that will receive callbacks.
      topic: The topic to subscribe to.
      verify_token: The verification token to use to confirm the
        subscription request.
      secret: Shared secret used for HMACs.
      hash_func: String with the name of the hash function to use for HMACs.
      lease_seconds: Number of seconds the client would like the subscription
        to last before expiring. Must be a number.
      now: Callable that returns the current time as a datetime instance. Used
        for testing

    Returns:
      True if the subscription was newly created, False otherwise.
    """
    key_name = cls.create_key_name(callback, topic)
    def txn():
      sub_is_new = False
      sub = cls.get_by_key_name(key_name)
      if sub is None:
        sub_is_new = True
        sub = cls(key_name=key_name,
                  callback=callback,
                  callback_hash=sha1_hash(callback),
                  topic=topic,
                  topic_hash=sha1_hash(topic),
                  verify_token=verify_token,
                  secret=secret,
                  hash_func=hash_func,
                  lease_seconds=lease_seconds,
                  expiration_time=(
                      now() + datetime.timedelta(seconds=lease_seconds)))
      sub.subscription_state = cls.STATE_VERIFIED
      sub.put()
      return sub_is_new
    return db.run_in_transaction(txn)

  @classmethod
  def request_insert(cls,
                     callback,
                     topic,
                     verify_token,
                     secret,
                     hash_func='sha1',
                     lease_seconds=DEFAULT_LEASE_SECONDS,
                     now=datetime.datetime.now):
    """Records that a callback URL needs verification before being subscribed.

    Creates a new subscription request (for asynchronous verification) if None
    already exists. Any existing subscription request will not be modified;
    for instance, if a subscription has already been verified, this method
    will do nothing.

    Args:
      callback: URL that will receive callbacks.
      topic: The topic to subscribe to.
      verify_token: The verification token to use to confirm the
        subscription request.
      secret: Shared secret used for HMACs.
      hash_func: String with the name of the hash function to use for HMACs.
      lease_seconds: Number of seconds the client would like the subscription
        to last before expiring. Must be a number.
      now: Callable that returns the current time as a datetime instance. Used
        for testing

    Returns:
      True if the subscription request was newly created, False otherwise.
    """
    key_name = cls.create_key_name(callback, topic)
    def txn():
      sub_is_new = False
      sub = cls.get_by_key_name(key_name)
      # TODO(bslatkin): Allow for a re-confirmation of an existing subscription
      # without affecting the serving state of the existing one. This is
      # required in situations where users want to renew their existing
      # subscriptions before the lease period has elapsed.
      if sub is None:
        sub_is_new = True
        sub = cls(key_name=key_name,
                  callback=callback,
                  callback_hash=sha1_hash(callback),
                  topic=topic,
                  topic_hash=sha1_hash(topic),
                  secret=secret,
                  hash_func=hash_func,
                  verify_token=verify_token,
                  lease_seconds=lease_seconds,
                  expiration_time=(
                      now() + datetime.timedelta(seconds=lease_seconds)))
        sub.put()
      return (sub_is_new, sub)
    new, sub = db.run_in_transaction(txn)
    # Note: This enqueuing must come *after* the transaction is submitted, or
    # else we'll actually run the task *before* the transaction is submitted.
    if new:
      sub._enqueue_task()
    return new

  @classmethod
  def remove(cls, callback, topic):
    """Causes a callback URL to no longer be subscribed to a topic.

    If the callback was not already subscribed to the topic, this method
    will do nothing. Otherwise, the subscription will immediately be removed.

    Args:
      callback: URL that will receive callbacks.
      topic: The topic to subscribe to.

    Returns:
      True if the subscription had previously existed, False otherwise.
    """
    key_name = cls.create_key_name(callback, topic)
    def txn():
      sub = cls.get_by_key_name(key_name)
      if sub is not None:
        sub.delete()
        return True
      return False
    return db.run_in_transaction(txn)

  @classmethod
  def request_remove(cls, callback, topic, verify_token):
    """Records that a callback URL needs to be unsubscribed.

    Creates a new request to unsubscribe a callback URL from a topic (where
    verification should happen asynchronously). If an unsubscribe request
    has already been made, this method will do nothing.

    Args:
      callback: URL that will receive callbacks.
      topic: The topic to subscribe to.
      verify_token: The verification token to use to confirm the
        unsubscription request.

    Returns:
      True if the unsubscribe request is new, False otherwise (i.e., a request
      for asynchronous unsubscribe was already made).
    """
    key_name = cls.create_key_name(callback, topic)
    def txn():
      sub = cls.get_by_key_name(key_name)
      if sub is not None and sub.subscription_state != cls.STATE_TO_DELETE:
        sub.subscription_state = cls.STATE_TO_DELETE
        sub.verify_token = verify_token
        sub.put()
        return (True, sub)
      return (False, sub)
    removed, sub = db.run_in_transaction(txn)
    # Note: This enqueuing must come *after* the transaction is submitted, or
    # else we'll actually run the task *before* the transaction is submitted.
    if removed:
      sub._enqueue_task()
    return removed

  @classmethod
  def has_subscribers(cls, topic):
    """Check if a topic URL has verified subscribers.

    Args:
      topic: The topic URL to check for subscribers.

    Returns:
      True if it has verified subscribers, False otherwise.
    """
    if (cls.all().filter('topic_hash =', sha1_hash(topic))
        .filter('subscription_state = ', cls.STATE_VERIFIED).get() is not None):
      return True
    else:
      return False

  @classmethod
  def get_subscribers(cls, topic, count, starting_at_callback=None):
    """Gets the list of subscribers starting at an offset.

    Args:
      topic: The topic URL to retrieve subscribers for.
      count: How many subscribers to retrieve.
      starting_at_callback: A string containing the callback hash to offset
        to when retrieving more subscribers. The callback at the given offset
        *will* be included in the results. If None, then subscribers will
        be retrieved from the beginning.

    Returns:
      List of Subscription objects that were found, or an empty list if none
      were found.
    """
    query = cls.all()
    query.filter('topic_hash =', sha1_hash(topic))
    query.filter('subscription_state = ', cls.STATE_VERIFIED)
    if starting_at_callback:
      query.filter('callback_hash >=', sha1_hash(starting_at_callback))
    query.order('callback_hash')

    return query.fetch(count)

  def _enqueue_task(self):
    """Enqueues a task to confirm this Subscription."""
    # TODO(bslatkin): Remove these retries when they're not needed in userland.
    RETRIES = 3
    for i in xrange(RETRIES):
      try:
        taskqueue.Task(
            url='/work/subscriptions',
            eta=self.eta,
            params={'subscription_key_name': self.key().name()}
            ).add(SUBSCRIPTION_QUEUE)
      except (taskqueue.Error, apiproxy_errors.Error):
        logging.exception('Could not insert task to confirm '
                          'topic = %s, callback = %s',
                          self.topic, self.callback)
        if i == (RETRIES - 1):
          raise
      else:
        return

  def confirm_failed(self, max_failures=MAX_SUBSCRIPTION_CONFIRM_FAILURES,
                     retry_period=SUBSCRIPTION_RETRY_PERIOD,
                     now=datetime.datetime.utcnow):
    """Reports that an asynchronous confirmation request has failed.

    This will delete this entity if the maximum number of failures has been
    exceeded.

    Args:
      max_failures: Maximum failures to allow before giving up.
      retry_period: Initial period for doing exponential (base-2) backoff.
      now: Returns the current time as a UTC datetime.

    Returns:
      True if this Subscription confirmation should be retried again; in this
      case the caller should use the 'eta' field to insert the next Task for
      confirming the subscription. Returns False if we should give up and never
      try again.
    """
    if self.confirm_failures >= max_failures:
      logging.warning('Max subscription failures exceeded, giving up.')
      self.delete()
    else:
      retry_delay = retry_period * (2 ** self.confirm_failures)
      self.eta = now() + datetime.timedelta(seconds=retry_delay)
      self.confirm_failures += 1
      self.put()
      # TODO(bslatkin): Do this enqueuing transactionally.
      self._enqueue_task()

  @classmethod
  def get_confirm_work(cls, confirm_key_name):
    """Retrieves a Subscription to verify or remove asynchronously.

    Args:
      confirm_key_name: Key name of the Subscription entity to verify.

    Returns:
      The Subscription instance, or None if it is not available or already has
      been confirmed. The returned instance needs to have its status updated
      by confirming the subscription is still desired by the callback URL.
    """
    CONFIRM_STATES = (cls.STATE_NOT_VERIFIED, cls.STATE_TO_DELETE)
    sub = cls.get_by_key_name(confirm_key_name)
    if sub is not None and sub.subscription_state in CONFIRM_STATES:
      return sub
    else:
      return None


class FeedToFetch(db.Model):
  """A feed that has new data that needs to be pulled.

  The key name of this entity is a get_hash_key_name() hash of the topic URL, so
  multiple inserts will only ever write a single entity.
  """

  topic = db.TextProperty(required=True)
  eta = db.DateTimeProperty(auto_now_add=True)
  fetching_failures = db.IntegerProperty(default=0)
  totally_failed = db.BooleanProperty(default=False)

  # TODO(bslatkin): Add fetching failure reason (urlfetch, parsing, etc) and
  # surface it on the topic details page.

  @classmethod
  def get_by_topic(cls, topic):
    """Retrives a FeedToFetch by the topic URL.

    Args:
      topic: The URL for the feed.

    Returns:
      The FeedToFetch or None if it does not exist.
    """
    return cls.get_by_key_name(get_hash_key_name(topic))

  @classmethod
  def insert(cls, topic_list):
    """Inserts a set of FeedToFetch entities for a set of topics.

    Overwrites any existing entities that are already there.

    Args:
      topic_list: List of the topic URLs of feeds that need to be fetched.
    """
    if not topic_list:
      return
    feed_list = [cls(key_name=get_hash_key_name(topic), topic=topic)
                 for topic in set(topic_list)]
    db.put(feed_list)
    # TODO(bslatkin): Use a bulk interface or somehow merge combined fetches
    # into a single task.
    for feed in feed_list:
      feed._enqueue_task()

  def fetch_failed(self, max_failures=MAX_FEED_PULL_FAILURES,
                   retry_period=FEED_PULL_RETRY_PERIOD,
                   now=datetime.datetime.utcnow):
    """Reports that feed fetching failed.

    This will mark this feed as failing to fetch. This feed will not be
    refetched until insert() is called again.

    Args:
      max_failures: Maximum failures to allow before giving up.
      retry_period: Initial period for doing exponential (base-2) backoff.
      now: Returns the current time as a UTC datetime.
    """
    if self.fetching_failures >= max_failures:
      logging.warning('Max fetching failures exceeded, giving up.')
      self.totally_failed = True
      self.put()
    else:
      retry_delay = retry_period * (2 ** self.fetching_failures)
      logging.warning('Fetching failed. Will retry in %s seconds', retry_delay)
      self.eta = now() + datetime.timedelta(seconds=retry_delay)
      self.fetching_failures += 1
      self.put()
      # TODO(bslatkin): Do this enqueuing transactionally.
      self._enqueue_task()

  def done(self):
    """The feed fetch has completed successfully.

    This will delete this FeedToFetch entity iff the ETA has not changed,
    meaning a subsequent publish event did not happen for this topic URL. If
    the ETA has changed, then we can safely assume there is a pending Task to
    take care of this FeedToFetch and we should leave the entry.

    Returns:
      True if the entity was deleted, False otherwise.
    """
    def txn():
      other = db.get(self.key())
      if other and other.eta == self.eta:
        other.delete()
        return True
      else:
        return False
    return db.run_in_transaction(txn)

  def _enqueue_task(self):
    """Enqueues a task to fetch this feed."""
    # TODO(bslatkin): Remove these retries when they're not needed in userland.
    RETRIES = 3
    target_queue = os.environ.get('X_APPENGINE_QUEUENAME', FEED_QUEUE)
    for i in xrange(RETRIES):
      try:
        taskqueue.Task(
            url='/work/pull_feeds',
            eta=self.eta,
            params={'topic': self.topic}
            ).add(target_queue)
      except (taskqueue.Error, apiproxy_errors.Error):
        logging.exception('Could not insert task to fetch topic = %s',
                          self.topic)
        if i == (RETRIES - 1):
          raise
      else:
        return


class FeedRecord(db.Model):
  """Represents record of the feed from when it has been polled.

  This contains everything in a feed except for the entry data. That means any
  footers, top-level XML elements, namespace declarations, etc, will be
  captured in this entity.

  The key name of this entity is a get_hash_key_name() of the topic URL.
  """

  topic = db.TextProperty(required=True)
  header_footer = db.TextProperty()  # Save this for debugging.
  last_updated = db.DateTimeProperty(auto_now=True)  # The last polling time.

  # Content-related headers.
  content_type = db.TextProperty()
  last_modified = db.TextProperty()
  etag = db.TextProperty()

  @staticmethod
  def create_key_name(topic):
    """Creates a key name for a FeedRecord for a topic.

    Args:
      topic: The topic URL for the FeedRecord.

    Returns:
      String containing the key name.
    """
    return get_hash_key_name(topic)

  @classmethod
  def get_or_create(cls, topic):
    """Retrieves a FeedRecord by its topic or creates it if non-existent.

    Args:
      topic: The topic URL to retrieve the FeedRecord for.

    Returns:
      The FeedRecord found for this topic or a new one if it did not already
      exist.
    """
    return cls.get_or_insert(FeedRecord.create_key_name(topic), topic=topic)

  def update(self, headers, header_footer=None):
    """Updates the polling record of this feed.

    This method will *not* insert this instance into the Datastore.

    Args:
      headers: Dictionary of response headers from the feed that should be used
        to determine how to poll the feed in the future.
      header_footer: Contents of the feed's XML document minus the entry data;
        if not supplied, the old value will remain.
    """
    self.content_type = headers.get('Content-Type', '').lower()
    self.last_modified = headers.get('Last-Modified')
    self.etag = headers.get('ETag')
    if header_footer is not None:
      self.header_footer = header_footer

  def get_request_headers(self):
    """Returns the request headers that should be used to pull this feed.

    Returns:
      Dictionary of request header values.
    """
    headers = {
      'Cache-Control': 'no-cache no-store max-age=1',
      'Connection': 'cache-control',
    }
    if self.last_modified:
      headers['If-Modified-Since'] = self.last_modified
    if self.etag:
      headers['If-None-Match'] = self.etag
    return headers


class FeedEntryRecord(db.Model):
  """Represents a feed entry that has been seen.

  The key name of this entity is a get_hash_key_name() hash of the combination
  of the topic URL and the entry_id.
  """

  entry_id = db.TextProperty(required=True)  # To allow 500+ length entry IDs.
  entry_id_hash = db.StringProperty(required=True)
  entry_content_hash = db.StringProperty()
  update_time = db.DateTimeProperty(auto_now=True)

  @classmethod
  def create_key(cls, topic, entry_id):
    """Creates a new Key for a FeedEntryRecord entity.

    Args:
      topic: The topic URL to retrieve entries for.
      entry_id: String containing the entry_id.

    Returns:
      Key instance for this FeedEntryRecord.
    """
    return db.Key.from_path(
        FeedRecord.kind(),
        FeedRecord.create_key_name(topic),
        cls.kind(),
        get_hash_key_name(entry_id))

  @classmethod
  def get_entries_for_topic(cls, topic, entry_id_list):
    """Gets multiple FeedEntryRecord entities for a topic by their entry_ids.

    Args:
      topic: The topic URL to retrieve entries for.
      entry_id_list: Sequence of entry_ids to retrieve.

    Returns:
      List of FeedEntryRecords that were found, if any.
    """
    results = cls.get([cls.create_key(topic, entry_id)
                       for entry_id in entry_id_list])
    # Filter out those pesky Nones.
    return [r for r in results if r]

  @classmethod
  def create_entry_for_topic(cls, topic, entry_id, content_hash):
    """Creates multiple FeedEntryRecords entities for a topic.

    Does not actually insert the entities into the Datastore. This is left to
    the caller so they can do it as part of a larger batch put().

    Args:
      topic: The topic URL to insert entities for.
      entry_id: String containing the ID of the entry.
      content_hash: Sha1 hash of the entry's entire XML content. For example,
        with Atom this would apply to everything from <entry> to </entry> with
        the surrounding tags included. With RSS it would be everything from
        <item> to </item>.

    Returns:
      A new FeedEntryRecord that should be inserted into the Datastore.
    """
    key = cls.create_key(topic, entry_id)
    return cls(key_name=key.name(),
               parent=key.parent(),
               entry_id=entry_id,
               entry_id_hash=sha1_hash(entry_id),
               entry_content_hash=content_hash)


class EventToDeliver(db.Model):
  """Represents a publishing event to deliver to subscribers.

  This model is meant to be used together with Subscription entities. When a
  feed has new published data and needs to be pushed to subscribers, one of
  these entities will be inserted. The background worker should iterate
  through all Subscription entities for this topic, sending them the event
  payload. The update() method should be used to track the progress of the
  background worker as well as any Subscription entities that failed delivery.

  The key_name for each of these entities is unique. It is up to the event
  injection side of the system to de-dupe events to deliver. For example, when
  a publish event comes in, that publish request should be de-duped immediately.
  Later, when the feed puller comes through to grab feed diffs, it should insert
  a single event to deliver, collapsing any overlapping publish events during
  the delay from publish time to feed pulling time.
  """

  DELIVERY_MODES = ('normal', 'retry')
  NORMAL = 'normal'
  RETRY = 'retry'

  topic = db.TextProperty(required=True)
  topic_hash = db.StringProperty(required=True)
  payload = db.TextProperty(required=True)
  last_callback = db.TextProperty(default='')  # For paging Subscriptions
  failed_callbacks = db.ListProperty(db.Key)  # Refs to Subscription entities
  delivery_mode = db.StringProperty(default=NORMAL, choices=DELIVERY_MODES)
  retry_attempts = db.IntegerProperty(default=0)
  last_modified = db.DateTimeProperty(required=True)
  totally_failed = db.BooleanProperty(default=False)
  content_type = db.TextProperty(default='')

  @classmethod
  def create_event_for_topic(cls, topic, format, header_footer, entry_payloads,
                             now=datetime.datetime.utcnow):
    """Creates an event to deliver for a topic and set of published entries.

    Args:
      topic: The topic that had the event.
      format: Format of the feed, either 'atom' or 'rss'.
      header_footer: The header and footer of the published feed into which
        the entry list will be spliced.
      entry_payloads: List of strings containing entry payloads (i.e., all
        XML data for each entry, including surrounding tags) in order of newest
        to oldest.
      now: Returns the current time as a UTC datetime.

    Returns:
      A new EventToDeliver instance that has not been stored.
    """
    if format == ATOM:
      close_tag = '</feed>'
      content_type = 'application/atom+xml'
    elif format == RSS:
      close_tag = '</channel>'
      content_type = 'application/rss+xml'
    else:
      assert False, 'Invalid format "%s"' % format

    close_index = header_footer.rfind(close_tag)
    assert close_index != -1, 'Could not find %s in feed envelope' % close_tag
    payload_list = ['<?xml version="1.0" encoding="utf-8"?>',
                    header_footer[:close_index]]
    payload_list.extend(entry_payloads)
    payload_list.append(header_footer[close_index:])
    payload = '\n'.join(payload_list)

    return cls(
        parent=db.Key.from_path(
            FeedRecord.kind(), FeedRecord.create_key_name(topic)),
        topic=topic,
        topic_hash=sha1_hash(topic),
        payload=payload,
        last_modified=now(),
        content_type=content_type)

  def get_next_subscribers(self, chunk_size=None):
    """Retrieve the next set of subscribers to attempt delivery for this event.

    Args:
      chunk_size: How many subscribers to retrieve at a time while delivering
        the event. Defaults to EVENT_SUBSCRIBER_CHUNK_SIZE.

    Returns:
      Tuple (more_subscribers, subscription_list) where:
        more_subscribers: True if there are more subscribers to deliver to
          after the returned 'subscription_list' has been contacted; this value
          should be passed to update() after the delivery is attempted.
        subscription_list: List of Subscription entities to attempt to contact
          for this event.
    """
    if chunk_size is None:
      chunk_size = EVENT_SUBSCRIBER_CHUNK_SIZE

    if self.delivery_mode == EventToDeliver.NORMAL:
      all_subscribers = Subscription.get_subscribers(
          self.topic, chunk_size + 1, starting_at_callback=self.last_callback)
      if all_subscribers:
        self.last_callback = all_subscribers[-1].callback
      else:
        self.last_callback = ''

      more_subscribers = len(all_subscribers) > chunk_size
      subscription_list = all_subscribers[:chunk_size]
    elif self.delivery_mode == EventToDeliver.RETRY:
      next_chunk = self.failed_callbacks[:chunk_size]
      more_subscribers = len(self.failed_callbacks) > len(next_chunk)

      if self.last_callback:
        # If the final index is present in the next chunk, that means we've
        # wrapped back around to the beginning and will need to do more
        # exponential backoff. This also requires updating the last_callback
        # in the update() method, since we do not know which callbacks from
        # the next chunk will end up failing.
        final_subscription_key = datastore_types.Key.from_path(
            Subscription.__name__,
            Subscription.create_key_name(self.last_callback, self.topic))
        try:
          final_index = next_chunk.index(final_subscription_key)
        except ValueError:
          pass
        else:
          more_subscribers = False
          next_chunk = next_chunk[:final_index]

      subscription_list = [x for x in db.get(next_chunk) if x is not None]
      if subscription_list and not self.last_callback:
        # This must be the first time through the current iteration where we do
        # not yet know a sentinal value in the list that represents the starting
        # point.
        self.last_callback = subscription_list[0].callback

      # If the failed callbacks fail again, they will be added back to the
      # end of the list.
      self.failed_callbacks = self.failed_callbacks[len(next_chunk):]

    return more_subscribers, subscription_list

  def update(self,
             more_callbacks,
             more_failed_callbacks,
             now=datetime.datetime.utcnow,
             max_failures=MAX_DELIVERY_FAILURES,
             retry_period=DELIVERY_RETRY_PERIOD):
    """Updates an event with work progress or deletes it if it's done.

    Reschedules another Task to run to handle this event delivery if needed.

    Args:
      more_callbacks: True if there are more callbacks to deliver, False if
        there are no more subscribers to deliver for this feed.
      more_failed_callbacks: Iterable of Subscription entities for this event
        that failed to deliver.
      max_failures: Maximum failures to allow before giving up.
      retry_period: Initial period for doing exponential (base-2) backoff.
      now: Returns the current time as a UTC datetime.
    """
    self.last_modified = now()

    # Ensure the list of failed callbacks is in sorted order so we keep track
    # of the last callback seen in alphabetical order of callback URL hashes.
    more_failed_callbacks = sorted(more_failed_callbacks,
                                   key=lambda x: x.callback_hash)

    self.failed_callbacks.extend(e.key() for e in more_failed_callbacks)
    if not more_callbacks and not self.failed_callbacks:
      logging.info('EventToDeliver complete: topic = %s, delivery_mode = %s',
                   self.topic, self.delivery_mode)
      self.delete()
      return
    elif not more_callbacks:
      self.last_callback = ''
      retry_delay = retry_period * (2 ** self.retry_attempts)
      self.last_modified += datetime.timedelta(seconds=retry_delay)
      self.retry_attempts += 1
      if self.retry_attempts > max_failures:
        self.totally_failed = True

      if self.delivery_mode == EventToDeliver.NORMAL:
        logging.warning('Normal delivery done; %d broken callbacks remain',
                        len(self.failed_callbacks))
        self.delivery_mode = EventToDeliver.RETRY
      else:
        logging.warning('End of attempt %d; topic = %s, subscribers = %d, '
                        'waiting until %s or totally_failed = %s',
                        self.retry_attempts, self.topic,
                        len(self.failed_callbacks), self.last_modified,
                        self.totally_failed)

    self.put()
    if not self.totally_failed:
      # TODO(bslatkin): Do this enqueuing transactionally.
      self.enqueue()

  def enqueue(self):
    """Enqueues a Task that will execute this EventToDeliver."""
    # TODO(bslatkin): Remove these retries when they're not needed in userland.
    RETRIES = 3
    for i in xrange(RETRIES):
      try:
        taskqueue.Task(
            url='/work/push_events',
            eta=self.last_modified,
            params={'event_key': self.key()}
            ).add(EVENT_QUEUE)
      except (taskqueue.Error, apiproxy_errors.Error):
        logging.exception('Could not insert task to deliver '
                          'events for topic = %s', self.topic)
        if i == (RETRIES - 1):
          raise
      else:
        return


class KnownFeed(db.Model):
  """Represents a feed that we know exists.

  This entity will be overwritten anytime someone subscribes to this feed. The
  benefit is we have a single entity per known feed, allowing us to quickly
  iterate through all of them. This may have issues if the subscription rate
  for a single feed is over one per second.
  """

  topic = db.TextProperty(required=True)

  @classmethod
  def create(cls, topic):
    """Creates a new KnownFeed.

    Args:
      topic: The feed's topic URL.

    Returns:
      The KnownFeed instance that hasn't been added to the Datastore.
    """
    return cls(key_name=get_hash_key_name(topic), topic=topic)

  @classmethod
  def create_key(cls, topic):
    """Creates a key for a KnownFeed.

    Args:
      topic: The feed's topic URL.

    Returns:
      Key instance for this feed.
    """
    return datastore_types.Key.from_path(cls.kind(), get_hash_key_name(topic))

  @classmethod
  def check_exists(cls, topics):
    """Checks if the supplied topic URLs are known feeds.

    Args:
      topics: Iterable of topic URLs.

    Returns:
      List of topic URLs with KnownFeed entries. If none are known, this list
      will be empty. The returned order is arbitrary.
    """
    result = []
    for known_feed in cls.get([cls.create_key(url) for url in set(topics)]):
      if known_feed is not None:
        result.append(known_feed.topic)
    return result


class PollingMarker(db.Model):
  """Keeps track of the current position in the bootstrap polling process."""

  last_start = db.DateTimeProperty()
  next_start = db.DateTimeProperty(required=True)

  @classmethod
  def get(cls, now=datetime.datetime.utcnow):
    """Returns the current PollingMarker, creating it if it doesn't exist.

    Args:
      now: Returns the current time as a UTC datetime.
    """
    key_name = 'The Mark'
    the_mark = db.get(datastore_types.Key.from_path(cls.kind(), key_name))
    if the_mark is None:
      next_start = now() - datetime.timedelta(seconds=60)
      the_mark = PollingMarker(key_name=key_name,
                               next_start=next_start,
                               current_key=None)
    return the_mark

  def should_progress(self,
                      period=POLLING_BOOTSTRAP_PERIOD,
                      now=datetime.datetime.utcnow):
    """Returns True if the bootstrap polling should progress.

    May modify this PollingMarker to when the next polling should start.

    Args:
      period: The poll period for bootstrapping.
      now: Returns the current time as a UTC datetime.
    """
    now_time = now()
    if self.next_start < now_time:
      logging.info('Polling starting afresh for start time %s', self.next_start)
      self.last_start = self.next_start
      self.next_start = now_time + datetime.timedelta(seconds=period)
      return True
    else:
      return False

################################################################################
# Subscription handlers and workers

def confirm_subscription(mode, topic, callback, verify_token,
                         secret, lease_seconds):
  """Confirms a subscription request and updates a Subscription instance.

  Args:
    mode: The mode of subscription confirmation ('subscribe' or 'unsubscribe').
    topic: URL of the topic being subscribed to.
    callback: URL of the callback handler to confirm the subscription with.
    verify_token: Opaque token passed to the callback.
    secret: Shared secret used for HMACs.
    lease_seconds: Number of seconds the client would like the subscription
      to last before expiring. If more than max_lease_seconds, will be capped
      to that value. Should be an integer number.

  Returns:
    True if the subscription was confirmed properly, False if the subscription
    request encountered an error or any other error has hit.
  """
  logging.debug('Attempting to confirm %s for topic = %s, callback = %s, '
                'verify_token = %s, secret = %s, lease_seconds = %s',
                mode, topic, callback, verify_token, secret, lease_seconds)

  parsed_url = list(urlparse.urlparse(callback))
  challenge = get_random_challenge()
  real_lease_seconds = min(lease_seconds, MAX_LEASE_SECONDS)
  params = {
    'hub.mode': mode,
    'hub.topic': topic,
    'hub.challenge': challenge,
    'hub.lease_seconds': real_lease_seconds,
  }
  if verify_token:
    params['hub.verify_token'] = verify_token
  parsed_url[4] = urllib.urlencode(params)
  adjusted_url = urlparse.urlunparse(parsed_url)

  try:
    response = urlfetch.fetch(adjusted_url, method='get',
                              follow_redirects=False)
  except urlfetch_errors.Error:
    logging.exception('Error encountered while confirming subscription')
    return False

  if 200 <= response.status_code < 300 and response.content == challenge:
    if mode == 'subscribe':
      Subscription.insert(callback, topic, verify_token, secret,
                          lease_seconds=real_lease_seconds)
      # Blindly put the feed's record so we have a record of all feeds.
      db.put(KnownFeed.create(topic))
    else:
      Subscription.remove(callback, topic)
    logging.info('Subscription action verified: %s', mode)
    return True
  else:
    logging.warning('Could not confirm subscription; encountered '
                    'status %d with content: %s', response.status_code,
                    response.content)
    return False


class SubscribeHandler(webapp.RequestHandler):
  """End-user accessible handler for Subscribe and Unsubscribe events."""

  def get(self):
    self.response.out.write(template.render('subscribe_debug.html', {}))

  @dos.limit(param='hub.callback', count=10, period=1)
  def post(self):
    self.response.headers['Content-Type'] = 'text/plain'

    callback = self.request.get('hub.callback', '')
    topic = self.request.get('hub.topic', '')
    verify_type_list = [s.lower() for s in self.request.get_all('hub.verify')]
    verify_token = self.request.get('hub.verify_token', '')
    secret = self.request.get('hub.secret', None)
    lease_seconds = self.request.get('hub.lease_seconds',
                                     str(DEFAULT_LEASE_SECONDS))
    mode = self.request.get('hub.mode', '').lower()

    error_message = None
    if not callback or not is_valid_url(callback):
      error_message = 'Invalid parameter: hub.callback'
    if not topic or not is_valid_url(topic):
      error_message = 'Invalid parameter: hub.topic'

    supported_verify_types = ['async', 'sync']
    verify_type_list.append(None)
    verify_type = [vt for vt in verify_type_list if vt in supported_verify_types or vt is None][0]
    if not verify_type in supported_verify_types:
      error_message = 'Invalid values for hub.verify: %s' % (verify_type_list,)

    if mode not in ('subscribe', 'unsubscribe'):
      error_message = 'Invalid value for hub.mode: %s' % mode

    if lease_seconds:
      try:
        old_lease_seconds = lease_seconds
        lease_seconds = int(old_lease_seconds)
        if not old_lease_seconds == str(lease_seconds):
          raise ValueError
      except ValueError:
        error_message = ('Invalid value for hub.lease_seconds: %s' %
                         old_lease_seconds)

    if error_message:
      logging.debug('Bad request for mode = %s, topic = %s, '
                    'callback = %s, verify_token = %s, lease_seconds = %s: %s',
                    mode, topic, callback, verify_token,
                    lease_seconds, error_message)
      self.response.out.write(error_message)
      return self.response.set_status(400)

    try:
      # Retrieve any existing subscription for this callback.
      sub = Subscription.get_by_key_name(
          Subscription.create_key_name(callback, topic))

      # Deletions for non-existant subscriptions will be ignored.
      if mode == 'unsubscribe' and not sub:
        return self.response.set_status(204)

      # Enqueue a background verification task, or immediately confirm.
      # We prefer synchronous confirmation.
      if verify_type == 'sync':
        if hooks.execute(confirm_subscription,
              mode, topic, callback, verify_token, secret, lease_seconds):
          return self.response.set_status(204)
        else:
          self.response.out.write('Error trying to confirm subscription')
          return self.response.set_status(409)
      else:
        if mode == 'subscribe':
          Subscription.request_insert(callback, topic, verify_token, secret,
                                      lease_seconds=lease_seconds)
        else:
          Subscription.request_remove(callback, topic, verify_token)
        logging.debug('Queued %s request for callback = %s, '
                      'topic = %s, verify_token = "%s", lease_seconds= %s',
                      mode, callback, topic, verify_token, lease_seconds)
        return self.response.set_status(202)

    except (apiproxy_errors.Error, db.Error,
            runtime.DeadlineExceededError, taskqueue.Error):
      logging.exception('Could not verify subscription request')
      self.response.headers['Retry-After'] = '120'
      return self.response.set_status(503)


class SubscriptionConfirmHandler(webapp.RequestHandler):
  """Background worker for asynchronously confirming subscriptions."""

  @work_queue_only
  def post(self):
    sub_key_name = self.request.get('subscription_key_name')
    sub = Subscription.get_confirm_work(sub_key_name)
    if not sub:
      logging.debug('No subscriptions to confirm '
                    'for subscription_key_name = %s', sub_key_name)
      return

    if sub.subscription_state == Subscription.STATE_NOT_VERIFIED:
      mode = 'subscribe'
    else:
      mode = 'unsubscribe'

    if not hooks.execute(confirm_subscription,
        mode, sub.topic, sub.callback,
        sub.verify_token, sub.secret, sub.lease_seconds):
      sub.confirm_failed()

################################################################################
# Publishing handlers

def preprocess_urls(urls):
  """Preprocesses URLs doing any necessary canonicalization.

  Args:
    urls: Iterable of URLs.

  Returns:
    Iterable of URLs that have been modified.
  """
  return urls


class PublishHandlerBase(webapp.RequestHandler):
  """Base-class for publish ping receiving handlers."""

  def receive_publish(self, urls, success_code, param_name):
    """Receives a publishing event for a set of topic URLs.

    Serves 400 errors on invalid input, 503 retries on insertion failures.

    Args:
      urls: Iterable of URLs that have been published.
      success_code: HTTP status code to return on success.
      param_name: Name of the parameter that will be validated.

    Returns:
      The error message, or an empty string if there are no errors.
    """
    urls = hooks.execute(preprocess_urls, urls)
    for url in urls:
      if not is_valid_url(url):
        self.response.set_status(400)
        return '%s invalid: %s' % (param_name, url)

    # Only insert FeedToFetch entities for feeds that are known to have
    # subscribers. The rest will be ignored.
    urls = KnownFeed.check_exists(urls)
    if urls:
      logging.info('Topics with known subscribers: %s', urls)

    # Record all FeedToFetch requests here. The background Pull worker will
    # double-check if there are any subscribers that need event delivery and
    # will skip any unused feeds.
    try:
      FeedToFetch.insert(urls)
    except (apiproxy_errors.Error, db.Error, runtime.DeadlineExceededError):
      logging.exception('Failed to insert FeedToFetch records')
      self.response.headers['Retry-After'] = '120'
      self.response.set_status(503)
      return 'Transient error; please try again later'
    else:
      self.response.set_status(success_code)
      return ''


class PublishHandler(PublishHandlerBase):
  """End-user accessible handler for the Publish event."""

  def get(self):
    self.response.out.write(template.render('publish_debug.html', {}))

  @dos.limit(count=100, period=1)  # TODO(bslatkin): need whitelist
  def post(self):
    self.response.headers['Content-Type'] = 'text/plain'

    mode = self.request.get('hub.mode')
    if mode.lower() != 'publish':
      self.response.set_status(400)
      self.response.out.write('hub.mode MUST be "publish"')
      return

    urls = set(self.request.get_all('hub.url'))
    if not urls:
      self.response.set_status(400)
      self.response.out.write('MUST supply at least one hub.url parameter')
      return

    logging.debug('Publish event for %d URLs: %s', len(urls), urls)
    error = self.receive_publish(urls, 204, 'hub.url')
    if error:
      self.response.out.write(error)


class WeblogsPingHandler(PublishHandlerBase):
  """Handles weblogs.com-style pings."""

  # To protect gainst auto-discovery DoS attacks.
  @dos.limit(header=None, param='url', count=5, period=1)
  # To limit a single pinging host.
  @dos.limit(count=20, period=1)
  def get(self):
    """Handles REST pings."""
    self.response.headers['Content-Type'] = 'text/plain'
    name = self.request.get('name')
    url = self.request.get('url')
    
    if not name:
      self.response.out.write('Missing Weblogs.com REST parameter "name"')
      self.response.set_status(400)
      return
    if not url:
      self.response.out.write('Missing Weblogs.com REST parameter "url"')
      self.response.set_status(400)
      return

    logging.debug('Weblogs.com REST ping for %s', url)
    if not is_valid_url(url):
      self.response.set_status(400)
      self.response.out.write('url invalid: %s' % url)
      return

    found_urls = auto_discover_urls(url)
    error = self.receive_publish(found_urls, 200, 'url')
    if error:
      self.response.out.write(error)

  # To limit a single pinging host.
  @dos.limit(count=20, period=1)
  def post(self):
    """Handles XML-RPC pings."""
    try:
      params, method = xmlrpclib.loads(self.request.body)
    except:
      logging.debug('Invalid XML-RPC with body:\n%s', self.request.body)
      self.response.headers['Content-Type'] = 'text/plain'
      self.response.out.write('Content body not valid XML-RPC')
      self.response.set_status(400)
      return

    error = ''
    if method != 'weblogUpdates.ping':
      error = 'Invalid XML-RPC method: %s' % method
    elif len(params) < 2:
      error = 'Invalid number of XML-RPC params: %d' % len(params)
    elif len(params) >= 4 and not is_valid_url(params[3]):
      error = 'Invalid feed URL in extended XML-RPC ping: %s' % params[3]
    elif not is_valid_url(params[1]):
      error = 'Invalid blog URL in XML-RPC ping: %s' % params[1]
    else:
      blog_name, blog_url, unused, feed_url, unused = \
          (params + ['', '', ''])[:5]
      if feed_url:
        logging.debug('Weblogs.com extended XML-RPC ping for %s', feed_url)
        found_urls = [feed_url]
      else:
        logging.debug('Weblogs.com XML-RPC ping for %s', blog_url)
        # TODO(bslatkin): figure out how to rate-limit this
        found_urls = auto_discover_urls(url)
      error = self.receive_publish(found_urls, 200, 'unused')

    self.response.headers['Content-Type'] = 'text/xml'
    if error:
      self.response.out.write(WEBLOGS_XMLRPC_ERROR % error)
    else:
      self.response.out.write(WEBLOGS_XMLRPC_SUCCESS)

################################################################################
# Pulling

def find_feed_updates(topic, format, feed_content,
                      filter_feed=feed_diff.filter):
  """Determines the updated entries for a feed and returns their records.

  Args:
    topic: The topic URL of the feed.
    format: The string 'atom' or 'rss'.
    feed_content: The content of the feed, which may include unicode characters.
    filter_feed: Used for dependency injection.

  Returns:
    Tuple (header_footer, entry_list, entry_payloads) where:
      header_footer: The header/footer data of the feed.
      entry_list: List of FeedEntryRecord instances, if any, that represent
        the changes that have occurred on the feed. These records do *not*
        include the payload data for the entry.
      entry_payloads: List of strings containing entry payloads (i.e., the XML
        data for the Atom <entry> or <item>).

  Raises:
    xml.sax.SAXException if there is a parse error.
    feed_diff.Error if the feed could not be diffed for any other reason.
  """
  header_footer, entries_map = filter_feed(feed_content, format)

  # Find the new entries we've never seen before, and any entries that we
  # knew about that have been updated.
  existing_entries = FeedEntryRecord.get_entries_for_topic(
      topic, entries_map.keys())
  existing_dict = dict((e.entry_id, e.entry_content_hash)
                       for e in existing_entries if e)

  logging.debug('Retrieved %d feed entries, %d of which have been seen before',
                len(entries_map), len(existing_dict))

  entities_to_save = []
  entry_payloads = []
  for entry_id, new_content in entries_map.iteritems():
    new_content_hash = sha1_hash(new_content)
    # Mark the entry as new if the sha1 hash is different.
    try:
      old_content_hash = existing_dict[entry_id]
      if old_content_hash == new_content_hash:
        continue
    except KeyError:
      pass

    entry_payloads.append(new_content)
    entities_to_save.append(FeedEntryRecord.create_entry_for_topic(
        topic, entry_id, new_content_hash))

  return header_footer, entities_to_save, entry_payloads


def pull_feed(feed_to_fetch, headers):
  """Pulls a feed.

  Args:
    feed_to_fetch: FeedToFetch instance to pull.
    headers: Dictionary of headers to use for doing the feed fetch.

  Returns:
    Tuple (status_code, response_headers, content) where:
      status_code: The response status code.
      response_headers: Caseless dictionary of response headers.
      content: The body of the response.

  Raises:
    apiproxy_errors.Error if any RPC errors are encountered. urlfetch.Error if
    there are any fetching API errors.
  """
  # Specifically follow redirects here. Many feeds are often just redirects
  # to the actual feed contents or a distribution server.
  response = urlfetch.fetch(feed_to_fetch.topic,
                            headers=headers,
                            follow_redirects=True)
  return response.status_code, response.headers, response.content


def parse_feed(feed_record, headers, content):
  """Parses a feed's content, determines changes, enqueues notifications.

  This function will only enqueue new notifications if the feed has changed.

  Args:
    feed_record: The FeedRecord object of the topic that has new content.
    headers: Dictionary of response headers found during feed fetching (may
        be empty).
    content: The feed document possibly containing new entries.

  Returns:
    True if successfully parsed the feed content; False on error.
  """
  # The content-type header is extremely unreliable for determining the feed's
  # content-type. Using a regex search for "<rss" could work, but an RE is
  # just another thing to maintain. Instead, try to parse the content twice
  # and use any hints from the content-type as best we can. This has
  # a bias towards Atom content (let's cross our fingers!).
  # TODO(bslatkin): Do something more efficient.
  if 'rss' in (feed_record.content_type or ''):
    order = (RSS, ATOM)
  else:
    order = (ATOM, RSS)

  parse_failures = 0
  for format in order:
    # Parse the feed. If this fails we will give up immediately.
    try:
      header_footer, entities_to_save, entry_payloads = find_feed_updates(
          feed_record.topic, format, content)
      break
    except (xml.sax.SAXException, feed_diff.Error), e:
      logging.debug(
          'Could not get entries for content of %d bytes in format "%s": %s',
          len(content), format, e)
      parse_failures += 1

  if parse_failures == len(order):
    logging.error('Could not parse feed content')
    return False

  if not entities_to_save:
    logging.debug('No new entries found')
    event_to_deliver = None
  else:
    logging.info('Saving %d new/updated entries', len(entities_to_save))
    event_to_deliver = EventToDeliver.create_event_for_topic(
        feed_record.topic, format, header_footer, entry_payloads)
    entities_to_save.append(event_to_deliver)

  feed_record.update(headers, header_footer)
  entities_to_save.append(feed_record)

  # Doing this put in a transaction ensures that we have written all
  # FeedEntryRecords, updated the FeedRecord, and written the EventToDeliver
  # at the same time. Otherwise, if any of these fails individually we could
  # drop messages on the floor. If this transaction fails, the whole fetch
  # will be redone and find the same entries again (thus it is idempotent).
  db.run_in_transaction(lambda: db.put(entities_to_save))
  # TODO(bslatkin): Make this transactional with the call to work.done()
  # that happens in the PullFeedHandler.post() method.
  if event_to_deliver:
    event_to_deliver.enqueue()

  return True


class PullFeedHandler(webapp.RequestHandler):
  """Background worker for pulling feeds."""

  @work_queue_only
  def post(self):
    topic = self.request.get('topic')
    work = FeedToFetch.get_by_topic(topic)
    if not work:
      logging.debug('No feeds to fetch for topic = %s', topic)
      return

    if not Subscription.has_subscribers(work.topic):
      logging.debug('Ignoring event because there are no subscribers '
                    'for topic %s', work.topic)
      # If there are no subscribers then we should also delete the record of
      # this being a known feed. This will clean up after the periodic polling.
      # TODO(bslatkin): Remove possibility of race-conditions here, where a
      # user starts subscribing to a feed immediately at the same time we do
      # this kind of pruning.
      if work.done():
        db.delete(KnownFeed.create_key(work.topic))
      return

    logging.debug('Fetching topic %s', work.topic)
    feed_record = FeedRecord.get_or_create(work.topic)
    try:
      status_code, headers, content = hooks.execute(pull_feed,
          work, feed_record.get_request_headers())
    except (apiproxy_errors.Error, urlfetch.Error):
      logging.exception('Failed to fetch feed')
      work.fetch_failed()
      return

    if status_code not in (200, 304):
      logging.warning('Received bad status_code = %s', status_code)
      work.fetch_failed()
      return

    if status_code == 304:
      logging.debug('Feed publisher returned 304 response (cache hit)')
      work.done()
      return

    if parse_feed(feed_record, headers, content):
      work.done()
    else:
      work.fetch_failed()

################################################################################
# Event delivery

def push_event(sub, headers, payload, async_proxy, callback):
  """Pushes an event to a single subscriber using an asynchronous API call.

  Args:
    sub: The Subscription instance to push the event to.
    headers: Request headers to use when pushing the event.
    payload: The content body the request should have.
    async_proxy: AsyncAPIProxy to use for registering RPCs.
    callback: Python callable to execute on success or failure. This callback
      has the signature func(sub, result, exception) where sub is the
      Subscription instance, result is the urlfetch.Response instance, and
      exception is any exception encountered, if any.
  """
  urlfetch_async.fetch(sub.callback,
                       method='POST',
                       headers=headers,
                       payload=payload,
                       async_proxy=async_proxy,
                       callback=callback)


class PushEventHandler(webapp.RequestHandler):

  def __init__(self, now=datetime.datetime.utcnow):
    """Initializer."""
    webapp.RequestHandler.__init__(self)
    self.now = now

  @work_queue_only
  def post(self):
    work = EventToDeliver.get(self.request.get('event_key'))
    if not work:
      logging.debug('No events to deliver.')
      return

    # Retrieve the first N + 1 subscribers; note if we have more to contact.
    more_subscribers, subscription_list = work.get_next_subscribers()
    logging.info('%d more subscribers to contact for: '
                 'topic = %s, delivery_mode = %s',
                 len(subscription_list), work.topic, work.delivery_mode)

    # Keep track of successful callbacks. Do this instead of tracking broken
    # callbacks because the asynchronous API calls could be interrupted by a
    # deadline error. If that happens we'll want to mark all outstanding
    # callback urls as still pending.
    failed_callbacks = set(subscription_list)
    def callback(sub, result, exception):
      if exception or result.status_code not in (200, 204):
        logging.warning('Could not deliver to target url %s: '
                        'Exception = %r, status_code = %s',
                        sub.callback, exception,
                        getattr(result, 'status_code', 'unknown'))
      else:
        failed_callbacks.remove(sub)

    def create_callback(sub):
      return lambda *args: callback(sub, *args)

    payload_utf8 = work.payload.encode('utf-8')
    for sub in subscription_list:
      headers = {
        # TODO(bslatkin): Remove the 'or' here once migration is done.
        'Content-Type': work.content_type or 'text/xml',
        # XXX(bslatkin): add a better test for verify_token here.
        'X-Hub-Signature': 'sha1=%s' % sha1_hmac(
            sub.secret or sub.verify_token or '', payload_utf8),
      }
      hooks.execute(push_event,
          sub, headers, payload_utf8, async_proxy, create_callback(sub))

    try:
      async_proxy.wait()
    except runtime.DeadlineExceededError:
      logging.error('Could not finish all callbacks due to deadline. '
                    'Remaining are: %r', [s.callback for s in failed_callbacks])

    work.update(more_subscribers, failed_callbacks)

################################################################################

class PollBootstrapHandler(webapp.RequestHandler):
  """Boostrap handler automatically polls feeds."""

  @work_queue_only
  def get(self):
    the_mark = PollingMarker.get()
    if the_mark.should_progress():
      # Naming the task based on the current start time here allows us to
      # enqueue the *next* task in the polling chain before we've enqueued
      # any of the actual FeedToFetch tasks. This is great because it lets us
      # queue up a ton of tasks in parallel (since the task queue is reentrant).
      #
      # Without the task name present, each intermittent failure in the polling
      # chain would cause an *alternate* sequence of tasks to execute. This
      # causes exponential explosion in the number of tasks (think of an
      # NP diagram or the "multiverse" of time/space). Yikes.
      name = str(int(time.mktime(the_mark.last_start.utctimetuple())))
      try:
        taskqueue.Task(
            url='/work/poll_bootstrap',
            name=name, params=dict(sequence=name)).add(POLLING_QUEUE)
      except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
        logging.exception('Could not enqueue FIRST polling task')

      the_mark.put()

  @work_queue_only
  def post(self):
    sequence = self.request.get('sequence')
    current_key = self.request.get('current_key')
    logging.info('Handling polling for sequence = %s, current_key = %s',
                 sequence, current_key)

    query = KnownFeed.all()
    if current_key:
      query.filter('__key__ >', datastore_types.Key(current_key))
    known_feeds = query.fetch(BOOSTRAP_FEED_CHUNK_SIZE)

    if known_feeds:
      current_key = str(known_feeds[-1].key())
      logging.info('Found %s more feeds to poll, ended at %s',
                   len(known_feeds), known_feeds[-1].topic)
      try:
        taskqueue.Task(
            url='/work/poll_bootstrap',
            name='%s-%s' % (sequence, sha1_hash(current_key)),
            params=dict(sequence=sequence,
                        current_key=current_key)).add(POLLING_QUEUE)
      except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
        logging.exception('Could not enqueue continued polling task')

      FeedToFetch.insert([k.topic for k in known_feeds])
    else:
      logging.info('Polling cycle complete')
      current_key = None

################################################################################

class HubHandler(webapp.RequestHandler):
  """Handler to multiplex subscribe and publish events on the same URL."""

  def get(self):
    self.response.out.write(open('./welcome.html').read())

  def post(self):
    mode = self.request.get('hub.mode', '').lower()
    if mode == 'publish':
      handler = PublishHandler()
    elif mode in ('subscribe', 'unsubscribe'):
      handler = SubscribeHandler()
    else:
      self.response.set_status(400)
      self.response.out.write('hub.mode is invalid')
      return

    handler.initialize(self.request, self.response)
    handler.post()


class TopicDetailHandler(webapp.RequestHandler):
  """Handler that serves topic debugging information to end-users."""

  @dos.limit(count=5, period=60)
  def get(self):
    topic_url = self.request.get('hub.url')
    feed = FeedRecord.get_by_key_name(FeedRecord.create_key_name(topic_url))
    if not feed:
      self.response.set_status(400)
      context = {
        'topic_url': topic_url,
        'error': 'Could not find any record for topic URL: ' + topic_url,
      }
    else:
      context = {
        'topic_url': topic_url,
        'last_successful_fetch': feed.last_updated,
        'last_content_type': feed.content_type,
        'last_etag': feed.etag,
        'last_modified': feed.last_modified,
        'last_header_footer': feed.header_footer,
      }
      fetch = FeedToFetch.get_by_topic(topic_url)
      if fetch:
        context.update({
          'next_fetch': fetch.eta,
          'fetch_attempts': fetch.fetching_failures,
          'totally_failed': fetch.totally_failed,
        })
    self.response.out.write(template.render('topic_details.html', context))

################################################################################
# Hook system

class InvalidHookError(Exception):
  """A module has tried to access a hook for an unknown function."""


class Hook(object):
  """A conditional hook that overrides or modifies Hub behavior.

  Each Hook corresponds to a single Python callable that may be overridden
  by the hook system. Multiple Hooks may inspect or modify the parameters, but
  only a single callable may elect to actually handle the call. The inspect()
  method will be called for each hook in the order the hooks are imported
  by the HookManager. The final set of parameters will be passed to the
  targetted hook's __call__() method. If more than one Hook elects to execute
  a hooked function, a warning logging message be issued and the *first* Hook
  encountered will be executed.
  """

  def inspect(self, args, kwargs):
    """Inspects a hooked function's parameters, possibly modifying them.

    Args:
      args: List of positional arguments for the hook call.
      kwargs: Dictionary of keyword arguments for the hook call.

    Returns:
      True if this Hook should handle the call, False otherwise.
    """
    return False

  def __call__(self, *args, **kwargs):
    """Handles the hook call.

    Args:
      *args, **kwargs: Parameters matching the original function's signature.

    Returns:
      The return value expected by the original function.
    """
    assert False, '__call__ method not defined for %s' % self.__class__


class HookManager(object):
  """Manages registering and loading Hooks from external modules.

  Hook modules will have a copy of this 'main' module's contents in their
  globals dictionary and the Hooks class to be sub-classed. They will also
  have the 'register' method, which the hook module should use to register any
  Hook sub-classes that it defines.

  The 'register' method has the same signature as the _register method of
  this class, but without the leading 'filename' argument; that value is
  curried by the HookManager.
  """

  def __init__(self):
    """Initializer."""
    # Maps hook functions to a list of (filename, Hook) tuples.
    self._mapping = {}

  def load(self, hooks_path='hooks', globals_dict=None):
    """Loads all hooks from a particular directory.

    Args:
      hooks_path: Optional. Relative path to the application directory or
        absolute path to load hook modules from.
      globals_dict: Dictionary of global variables to use when loading the
        hook module. If None, defaults to the contents of this 'main' module.
        Only for use in testing!
    """
    if globals_dict is None:
      globals_dict = globals()

    hook_directory = os.path.join(os.getcwd(), hooks_path)
    module_list = os.listdir(hook_directory)
    for module_name in sorted(module_list):
      if not module_name.endswith('.py'):
        logging.debug('Skipping module %s', module_name)
        continue
      module_path = os.path.join(hook_directory, module_name)
      context_dict = globals_dict.copy()
      context_dict.update({
        'Hook': Hook,
        'register': lambda *a, **k: self._register(module_name, *a, **k)
      })
      logging.debug('Loading hook "%s" from %s', module_name, module_path)
      try:
        exec open(module_path) in context_dict
      except:
        logging.exception('Error loading hook "%s" from %s',
                          module_name, module_path)
        raise

  def declare(self, original):
    """Declares a function as being hookable.

    Args:
      original: Python callable that may be hooked.
    """
    self._mapping[original] = []

  def execute(self, original, *args, **kwargs):
    """Executes a hookable method, possibly invoking a registered Hook.

    Args:
      original: The original hooked callable.
      args: Positional arguments to pass to the callable.
      kwargs: Keyword arguments to pass to the callable.

    Returns:
      Whatever value is returned by the hooked call.
    """
    try:
      hook_list = self._mapping[original]
    except KeyError, e:
      raise InvalidHookError(e)

    modifiable_args = list(args)
    modifiable_kwargs = dict(kwargs)
    matches = []
    for filename, hook in hook_list:
      logging.debug('Inspecting args for %s by hook from module %s: '
                    'args=%r, kwargs=%r', original, filename,
                    modifiable_args, modifiable_kwargs)
      if hook.inspect(modifiable_args, modifiable_kwargs):
        matches.append((filename, hook))

    filename = __name__
    designated_hook = original
    if len(matches) >= 1:
      filename, designated_hook = matches[0]
      logging.debug('Using matched hook for %s from module %s',
                    original, filename)

    if len(matches) > 1:
      logging.critical(
          'Found multiple matching hooks for %s in files: %s. '
          'Will use the first hook encountered: %s',
          original, [f for (f, hook) in matches], filename)

    return designated_hook(*args, **kwargs)

  def _register(self, filename, original, hook):
    """Registers a Hook to inspect and potentially execute a hooked function.

    Args:
      filename: The name of the hook module this Hook is defined in.
      original: The Python callable of the original hooked function.
      hook: The Hook to register for this hooked function.

    Raises:
      InvalidHookError if the original hook function is not known.
    """
    try:
      self._mapping[original].append((filename, hook))
    except KeyError, e:
      raise InvalidHookError(e)

  def override_for_test(self, original, test):
    """Adds a hook function for testing.

    Args:
      original: The Python callable of the original hooked function.
      test: The callable to use to override the original for this hook function.
    """
    class OverrideHook(Hook):
      def inspect(self, args, kwargs):
        return True
      def __call__(self, *args, **kwargs):
        return test(*args, **kwargs)
    self._register(__name__, original, OverrideHook())

  def reset_for_test(self, original):
    """Clears the configured test hook for a hooked function.

    Args:
      original: The Python callable of the original hooked function.
    """
    self._mapping[original].pop()

################################################################################

HANDLERS = []


def modify_handlers(handlers):
  """Modifies the set of web request handlers.

  Args:
    handlers: List of (path_regex, webapp.RequestHandler) instances that are
      configured for this application.

  Returns:
    Modified list of handlers, with some possibly removed and others added.
  """
  return handlers


def main():
  global HANDLERS
  if not HANDLERS:
    HANDLERS = hooks.execute(modify_handlers, [
      (r'/', HubHandler),
      (r'/publish', PublishHandler),
      (r'/subscribe', SubscribeHandler),
      (r'/work/subscriptions', SubscriptionConfirmHandler),
      (r'/work/poll_bootstrap', PollBootstrapHandler),
      (r'/work/pull_feeds', PullFeedHandler),
      (r'/work/push_events', PushEventHandler),
      (r'/topic-details', TopicDetailHandler),
    ])
  application = webapp.WSGIApplication(HANDLERS, debug=DEBUG)
  wsgiref.handlers.CGIHandler().run(application)

################################################################################
# Declare and load external hooks.

hooks = HookManager()
hooks.declare(preprocess_urls)
hooks.declare(confirm_subscription)
hooks.declare(pull_feed)
hooks.declare(push_event)
hooks.declare(modify_handlers)
hooks.load()


if __name__ == '__main__':
  main()
