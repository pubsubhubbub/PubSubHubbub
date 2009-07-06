<?php

// a PHP client library for pubsubhubbub
// as defined at http://code.google.com/p/pubsubhubbub/
// written by Josh Fraser | joshfraser.com | josh@eventvue.com
// Released under Apache License 2.0

class Publisher {
    
    protected $hub_url;
    protected $last_response;
    
    // create a new Publisher
    public function __construct($hub_url) {
        
        if (!isset($hub_url))
            throw new Exception('Please specify a hub url');
        
        if (!preg_match("|^https?://|i",$hub_url)) 
            throw new Exception('The specified hub url does not appear to be valid: '.$hub_url);
            
        $this->hub_url = $hub_url;
    }

    // accepts either a single url or an array of urls
    public function publish_update($topic_urls, $http_function = false) {
        if (!isset($topic_urls))
            throw new Exception('Please specify a topic url');
        
        // check that we're working with an array
        if (!is_array($topic_urls)) {
            $topic_urls = array($topic_urls);
        }
        
        // set the mode to publish
        $post_string = "hub.mode=publish";
        // loop through each topic url 
        foreach ($topic_urls as $topic_url) {

            // lightweight check that we're actually working w/ a valid url
            if (!preg_match("|^https?://|i",$topic_url)) 
                throw new Exception('The specified topic url does not appear to be valid: '.$topic_url);
            
            // append the topic url parameters
            $post_string .= "&hub.url=".urlencode($topic_url);
        }
        
        // make the http post request and return true/false
        // easy to over-write to use your own http function
        if ($http_function)
            return $http_function($this->hub_url,$post_string);
        else
            return $this->http_post($this->hub_url,$post_string);
    }

    // returns any error message from the latest request
    public function last_response() {
        return $this->last_response;
    }
    
    // default http function that uses curl to post to the hub endpoint
    private function http_post($url, $post_string) {
        
        // add any additional curl options here
        $options = array(CURLOPT_URL => $url,
                         CURLOPT_POST => true,
                         CURLOPT_POSTFIELDS => $post_string,
                         CURLOPT_USERAGENT => "PubSubHubbub-Publisher-PHP/1.0");

    	$ch = curl_init();
    	curl_setopt_array($ch, $options);

        $response = curl_exec($ch);
        $this->last_response = $response;
        $info = curl_getinfo($ch);

        curl_close($ch);
        
        // all good
        if ($info['http_code'] == 204) 
            return true;
        return false;	
    }
}

?>