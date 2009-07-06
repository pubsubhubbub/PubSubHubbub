This PHP library for PubSubHubbub was written by Josh Fraser (joshfraser.com) and is released under the Apache 2.0 License

Usage:
// specify which hub you want to use. in this case we'll use the demo hub on app engine.
$hub_url = "http://pubsubhubbub.appspot.com/";

// create a new pubsubhubbub publisher
$p = new Publisher($hub_url);

// specify the feed that has been updated
$topic_url = "http://www.onlineaspect.com";

// notify the hub that the specified topic_url (ATOM feed) has been updated
// alternatively, publish_update() also accepts an array of topic urls
if ($p->publish_update($topic_url)) {
    echo "$topic_url was successfully published to $hub_url";
} else {
    echo "Ooops...";
    print_r($p->last_response());
}
 