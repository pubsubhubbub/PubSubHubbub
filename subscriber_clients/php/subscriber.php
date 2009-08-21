<?php

// a PHP client library for pubsubhubbub
// as defined at http://code.google.com/p/pubsubhubbub/
// written by Josh Fraser | joshfraser.com | josh@eventvue.com
// Released under Apache License 2.0

class Subscriber {
    
    // put your google key here
    // required if you want to use the google feed API to lookup RSS feeds
    protected $google_key = "";
    
    protected $hub_url;
    protected $callback_url;
    protected $credentials;
    // accepted values are "async" and "sync"
    protected $verify = "async"; 
    protected $verify_token;
    protected $lease_seconds;
    
    // create a new Subscriber (credentials added for SuperFeedr support)
    public function __construct($hub_url, $callback_url, $credentials = false) {
        
        if (!isset($hub_url))
            throw new Exception('Please specify a hub url');
        
        if (!preg_match("|^https?://|i",$hub_url)) 
            throw new Exception('The specified hub url does not appear to be valid: '.$hub_url);
            
        if (!isset($callback_url))
            throw new Exception('Please specify a callback');
            
        $this->hub_url = $hub_url;
        $this->callback_url = $callback_url;
        $this->credentials = $credentials;
    }
    
    // $use_regexp lets you choose whether to use google AJAX feed api (faster, but cached) or a regexp to read from site
    public function find_feed($url, $http_function = false) {
        // using google feed API
        $url = "http://ajax.googleapis.com/ajax/services/feed/lookup?key={$this->google_key}&v=1.0&q=".urlencode($url);
        // fetch the content 
        if ($http_function)
            $response = $http_function($url);
        else
            $response = $this->http($url);

        $result = json_decode($response, true);
        $rss_url = $result['responseData']['url'];
        return $rss_url;
    }
    
    public function subscribe($topic_url, $http_function = false) {
        return $this->change_subscription("subscribe", $topic_url, $http_function = false);
    }
    
    public function unsubscribe($topic_url, $http_function = false) {
        return $this->change_subscription("unsubscribe", $topic_url, $http_function = false);
    }

    // helper function since sub/unsub are handled the same way
    private function change_subscription($mode, $topic_url, $http_function = false) {
        if (!isset($topic_url))
            throw new Exception('Please specify a topic url');
            
         // lightweight check that we're actually working w/ a valid url
         if (!preg_match("|^https?://|i",$topic_url)) 
            throw new Exception('The specified topic url does not appear to be valid: '.$topic_url);
        
        // set the mode subscribe/unsubscribe
        $post_string = "hub.mode=".$mode;
        $post_string .= "&hub.callback=".urlencode($this->callback_url);
        $post_string .= "&hub.verify=".$this->verify;
        $post_string .= "&hub.verify_token=".$this->verify_token;
        $post_string .= "&hub.lease_seconds=".$this->lease_seconds;
    
        // append the topic url parameters
        $post_string .= "&hub.topic=".urlencode($topic_url);
        
        // make the http post request and return true/false
        // easy to over-write to use your own http function
        if ($http_function)
            return $http_function($this->hub_url,$post_string);
        else
            return $this->http($this->hub_url,$post_string);
    }
    
    // default http function that uses curl to post to the hub endpoint
    private function http($url, $post_string) {
        
        // add any additional curl options here
        $options = array(CURLOPT_URL => $url,
                         CURLOPT_USERAGENT => "PubSubHubbub-Subscriber-PHP/1.0",
                         CURLOPT_RETURNTRANSFER => true);
                         
        if ($post_string) {
            $options[CURLOPT_POST] = true;
            $options[CURLOPT_POSTFIELDS] = $post_string;
        }
                         
        if ($this->credentials)
            $options[CURLOPT_USERPWD] = $this->credentials;

    	$ch = curl_init();
    	curl_setopt_array($ch, $options);

        $response = curl_exec($ch);
        $info = curl_getinfo($ch);
        
        // all good -- anything in the 200 range 
        if (substr($info['http_code'],0,1) == "2") {
            return $response;
        }
        return false;	
    }
}


?>