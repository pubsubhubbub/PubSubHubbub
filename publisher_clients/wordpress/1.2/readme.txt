=== Plugin Name ===
Contributors: joshfraz
Donate link: https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=5426516
Tags: pubsubhubbub
Requires at least: 2.5
Tested up to: 2.8.4
Stable tag: /trunk/

A better way to tell the world when your blog is updated.

== Description ==

This [PubSubHubbub](http://code.google.com/p/pubsubhubbub/ "PubSubHubbub") plugin is a simple way to let people know in real-time when your blog is updated.  PubSubHubbub is quickly gaining adoption and is already being used by Google Reader, Google Alerts, FriendFeed and more. 

This plugin:
 
* Now supports multiple hubs!   
* Supports all of the feed formats used by WordPress, not just ATOM and RSS2
* Announces which hubs you are using by adding `<link rel="hub" ...>` declarations to your template header and ATOM feed
* Adds `<atom:link rel="hub" ...>` to your RSS feeds along with the necessary XMLNS declaration for RSS 0.92/1.0

By default this plugin will ping the following hubs:
* [Demo hub on Google App Engine](http://pubsubhubbub.appspot.com "Demo hub on Google App Engine")
* [SuperFeedr](http://superfeedr.com/hubbub "SuperFeedr")

Please contact me if you operate a hub that you would like to be included.

== Installation ==

1. Upload the `pubsubhubbub` directory to your `/wp-content/plugins/` directory
2. Activate the plugin through the 'Plugins' menu in WordPress
3. Select custom hubs under your PubSubHubbub Settings (optional)

== Frequently Asked Questions ==

= Where can I learn more about the PubSubHubbub protocol? =

You can visit [PubSubHubbb on Google Code](http://code.google.com/p/pubsubhubbub/ "PubSubHubbb on Google Code")

= Where can I learn more about the author of this plugin? =

You can learn more about [Josh Fraser](http://www.joshfraser.com "Josh Fraser") at [Online Aspect](http://www.onlineaspect.com "Online Aspect")

== Screenshots ==

1. The PubSubHubbub Settings page allows you to define which hubs you want to use
