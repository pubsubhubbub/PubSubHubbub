<?php

// example usage for the PHP client library for pubsubhubbub
// as defined at http://code.google.com/p/pubsubhubbub/
// written by Josh Fraser | joshfraser.com | josh@eventvue.com
// Released under Apache License 2.0

include("../library/publisher.php");

echo "<center>";

// process form
if ($_POST['sub']) {
    
    $hub_url = $_POST['hub_url'];
    $topic_url = $_POST['topic_url'];
    
    // check that a hub url is specified
    if (!$hub_url) {
        echo "Please specify a hub url.<br /><br /><a href='publisher_example.php'>back</a>";
        exit();
    }
    // check that a topic url is specified
    if (!$topic_url) {
        echo "Please specify a topic url to publish.<br /><br /><a href='publisher_example.php'>back</a>";
        exit();
    }         
    
    // $hub_url = "http://pubsubhubbub.appspot.com/publish";
    $p = new Publisher($hub_url);
    if ($p->publish_update($topic_url)) {
        echo "<i>$topic_url</i> was successfully published to <i>$hub_url</i><br /><br /><a href='publisher_example.php'>back</a>";
    } else {
        echo "ooops...";
        print_r($p->last_response());
    }

} else {
    
    // display a primitive form for testing
    echo "<form action='publisher_example.php' method='POST'>";
    echo "hub url: <input name='hub_url' type='text' value='http://pubsubhubbub.appspot.com/publish' size='50'/><br />";
    echo "topic url: <input name='topic_url' type='text' value='http://www.onlineaspect.com' size='50' /><br />";
    echo "<input name='sub' type='submit' value='Publish' /><br />";
    echo "</form>";
    
}

echo "</center>";

?>