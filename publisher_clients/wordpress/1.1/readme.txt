=== Plugin Name ===
Contributors: joshfraz
Donate link: https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=5426516
Tags: pubsubhubbub
Requires at least: 2.5
Tested up to: 2.7
Stable tag: /trunk/

A better way to tell the world when your blog is updated.

== Description ==

This plugin that implements [the PubSubHubbub protocol](http://pubsubhubbub.googlecode.com/svn/trunk/pubsubhubbub-core-0.1.html "the PubSubHubbub protocol").  [PubSubHubbub](http://code.google.com/p/pubsubhubbub/ "PubSubHubbub") is a simple, open, server-to-server web-hook-based pubsub (publish/subscribe) protocol as a simple extension to Atom and RSS. 

Parties (servers) speaking the PubSubHubbub protocol can get near-instant notifications (via webhook callbacks) when a topic (feed URL) they're interested in is updated.

This plugin:
   
* Notifies your specified hub each time you publish a new post
* Announces your specified hub by adding `<link rel="hub" ...>` to your template header and ATOM feed
* Adds `<atom:link rel="hub" ...>` to your RSS feeds along with the necessary XMLNS declaration for RSS 0.92/1.0

The PubSubHubbub protocol is decentralized and free. No company is at the center of this controlling it. Anybody can run a hub, or anybody can ping (publish) or subscribe using open hubs.  If no custom hub is specified, this plugin will use the demonstration hub that is running on Google App Engine.  

== Installation ==

1. Upload the `pubsubhubbub` directory to your `/wp-content/plugins/` directory
2. Activate the plugin through the 'Plugins' menu in WordPress
3. Select a custom hub under your PubSubHubbub Settings (optional)

== Frequently Asked Questions ==

= Where can I learn more about the PubSubHubbub protocol? =

You can visit [PubSubHubbb on Google Code](http://code.google.com/p/pubsubhubbub/ "PubSubHubbb on Google Code")

= Where can I learn more about the author of this plugin? =

You can learn more about [Josh Fraser](http://www.joshfraser.com "Josh Fraser") at [Online Aspect](http://www.onlineaspect.com "Online Aspect")

== Screenshots ==

1. The PubSubHubbub Settings page allows you to define custom endpoints for your chosen hub
