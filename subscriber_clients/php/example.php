<?php

// simple example for the PHP pubsubhubbub Subscriber
// as defined at http://code.google.com/p/pubsubhubbub/
// written by Josh Fraser | joshfraser.com | josh@eventvue.com
// Released under Apache License 2.0

include("subscriber.php");

$hub_url = "http://pubsubhubbub.appspot.com";
$callback_url = "put your own endpoint here";

$feed = "http://feeds.feedburner.com/onlineaspect";

// create a new subscriber
$s = new Subscriber($hub_url, $callback_url);

// subscribe to a feed
$s->subscribe($feed);

// unsubscribe from a feed
$s->unsubscribe($feed);

?>

