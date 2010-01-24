<?php
/*
Plugin Name: PubSubHubbub
Plugin URI: http://code.google.com/p/pubsubhubbub/
Description: A better way to tell the world when your blog is updated. 
Version: 1.3
Author: Josh Fraser
Author Email: josh@eventvue.com
Author URI: http://www.joshfraser.com
*/

include("publisher.php");

// function that is called whenever a new post is published
function publish_to_hub($post_id)  {
    
    // we want to notify the hub for every feed
    $feed_urls = array();
    $feed_urls[] = get_bloginfo('atom_url');
    $feed_urls[] = get_bloginfo('rss_url');
    $feed_urls[] = get_bloginfo('rdf_url');
    $feed_urls[] = get_bloginfo('rss2_url');
    // remove dups (ie. they all point to feedburner)
    $feed_urls = array_unique($feed_urls);
    // get the list of hubs
    $hub_urls = get_pubsub_endpoints();
    // loop through each hub
    foreach ($hub_urls as $hub_url) {
        $p = new Publisher($hub_url);
        // publish the update to each hub
        if (!$p->publish_update($feed_urls, "http_post_wp")) {
            // TODO: add better error handling here
        }    
    }
    return $post_id;
}

function add_atom_link_tag() {    
    $hub_urls = get_pubsub_endpoints();
    foreach ($hub_urls as $hub_url) {
        echo '<link rel="hub" href="'.$hub_url.'" />';
    }
}

function add_rss_link_tag() {    
    $hub_urls = get_pubsub_endpoints();
    foreach ($hub_urls as $hub_url) {
        echo '<atom:link rel="hub" href="'.$hub_url.'"/>';
    }
}

function add_rdf_ns_link() {
    echo 'xmlns:atom="http://www.w3.org/2005/Atom"';
}

// hack to add the atom definition to the RSS feed
// start capturing the feed output.  this is run at priority 9 (before output)
function start_rss_link_tag() {    
    ob_start();
}

// this is run at priority 11 (after output)
// add in the xmlns atom definition link
function end_rss_link_tag() {    
    $feed = ob_get_clean();
    $pattern = '/<rss version="(.+)">/i';
    $replacement = '<rss version="$1" xmlns:atom="http://www.w3.org/2005/Atom">';
    // change <rss version="X.XX"> to <rss version="X.XX" xmlns:atom="http://www.w3.org/2005/Atom">
    echo preg_replace($pattern, $replacement, $feed);
}

// add a link to our settings page in the WP menu
function add_plugin_menu() {
    add_options_page('PubSubHubbub Settings', 'PubSubHubbub', 8, __FILE__, 'add_settings_page');
}

// get the endpoints from the wordpress options table
// valid parameters are "publish" or "subscribe"
function get_pubsub_endpoints() {
    $endpoints = get_option('pubsub_endpoints');
    $hub_urls = explode("\n",$endpoints);

    // if no values have been set, revert to the defaults (pubsubhubbub on app engine & superfeedr)
    if (!$endpoints) {
        $hub_urls[] = "http://pubsubhubbub.appspot.com";
        $hub_urls[] = "http://superfeedr.com/hubbub";
    }
    
    // clean out any blank values
    foreach ($hub_urls as $key => $value) {
        if (is_null($value) || $value=="") {
            unset($hub_urls[$key]);
        } else {
            $hub_urls[$key] = trim($hub_urls[$key]);
        }
    }
    
    return $hub_urls;
}

// write the content for our settings page that allows you to define your endpoints
function add_settings_page() { ?>
    <div class="wrap">
    <h2>Define custom hubs</h2>
    
    <form method="post" action="options.php">
    <?php //wp_nonce_field('update-options'); ?>
    <!-- starting -->
    <?php settings_fields('my_settings_group'); ?>
    <?php do_settings_sections('my_settings_section'); ?>
    <!-- ending -->
    
    <?php
    
    // load the existing pubsub endpoint list from the wordpress options table
    $pubsub_endpoints = trim(implode("\n",get_pubsub_endpoints()),"\n");
    
    ?>

    <table class="form-table">

    <tr valign="top">
    <th scope="row">Hubs (one per line)</th>
    <td><textarea name="pubsub_endpoints" style='width:600px;height:100px'><?php echo $pubsub_endpoints; ?></textarea></td>
    </tr>

    </table>

    <input type="hidden" name="action" value="update" />
    <input type="hidden" name="page_options" value="pubsub_endpoints" />

    <p class="submit">
    <input type="submit" class="button-primary" value="<?php _e('Save Changes') ?>" />
    </p>

    </form>
    
    <br /><br />
    <div style='background-color:#FFFEEB;border:1px solid #CCCCCC;padding:12px'>
        <strong>Thanks for using PubSubHubbub!</strong><br />
        Visit these links to learn more about PubSubHubbub and the author of this plugin:<br />
        <ul>
            <li><a href='http://www.onlineaspect.com'>Subscribe to Online Aspect</a></li>
            <li><a href='http://www.twitter.com/joshfraser'>Follow Josh Fraser on twitter</a></li>
            <li><a href='http://code.google.com/p/pubsubhubbub/'>Learn more about the PubSubHubbub protocol</a></li>
        </ul>
    </div>
    
    </div>

<?php }


// helper function to use the WP-friendly snoopy library 
if (!function_exists('get_snoopy')) {
	function get_snoopy() {
		include_once(ABSPATH.'/wp-includes/class-snoopy.php');
		return new Snoopy;
	}
}

// over-ride the default curl http function to post to the hub endpoints
function http_post_wp($url, $post_vars) {
    
    // turn the query string into an array for snoopy
    parse_str($post_vars);
    $post_vars = array();
    $post_vars['hub.mode'] = $hub_mode;  // PHP converts the periods to underscores
    $post_vars['hub.url'] = $hub_url;    
    
    // more universal than curl
    $snoopy = get_snoopy();
    $snoopy->agent = "(PubSubHubbub-Publisher-WP/1.0)";
	$snoopy->submit($url,$post_vars);
	$response = $snoopy->results;
	// TODO: store the last_response.  requires a litle refactoring work.
	$response_code = $snoopy->response_code;
	if ($response_code == 204)
	    return true;
    return false;
}

// add a settings link next to deactive / edit
function add_settings_link( $links, $file ) {
 	if( $file == 'pubsubhubbub/pubsubhubbub.php' && function_exists( "admin_url" ) ) {
		$settings_link = '<a href="' . admin_url( 'options-general.php?page=pubsubhubbub/pubsubhubbub' ) . '">' . __('Settings') . '</a>';
		array_unshift( $links, $settings_link ); // before other links
	}
	return $links;
}

// attach the handler that gets called every time you publish a post
add_action('publish_post', 'publish_to_hub');
// add the link to our settings page in the WP menu structure
add_action('admin_menu', 'add_plugin_menu');

// keep WPMU happy
add_action('admin_init', 'register_my_settings');
function register_my_settings() {
    register_setting('my_settings_group','pubsub_endpoints');
}

// add the link tag that points to the hub in the header of our template...

// to our atom feed
add_action('atom_head', 'add_atom_link_tag');
// to our RSS 0.92 feed (requires a bit of a hack to include the ATOM namespace definition)
add_action('do_feed_rss', 'start_rss_link_tag', 9); // run before output
add_action('do_feed_rss', 'end_rss_link_tag', 11); // run after output
add_action('rss_head', 'add_rss_link_tag');
// to our RDF / RSS 1 feed
add_action('rdf_ns', 'add_rdf_ns_link');
add_action('rdf_header', 'add_rss_link_tag');
// to our RSS 2 feed
add_action('rss2_head', 'add_rss_link_tag');
// to our main HTML header -- not sure if we want to include this long-term or not.
add_action('wp_head', 'add_atom_link_tag');

add_filter('plugin_action_links', 'add_settings_link', 10, 2);

?>