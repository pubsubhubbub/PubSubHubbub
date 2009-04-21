// Copyright 2009 Google Inc.
// 
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//      http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var _____pshb_BookmarkletRun = function() {

  var post_id = 'pshb-bookmarklet-iframe';
  var post = null;
  var close = null;

  if (document.getElementById(post_id) == null) {
    post = document.createElement('iframe');
    post.id = post_id;
    post.width = '250';
    post.height = '120';
    var s = post.style;
    s.position = 'absolute';
    s.top = '10px';
    s.right = '10px';
    s.padding = '0';
    s.margin = '0';
    s.border = '5px solid #9c0';
    s.zIndex = '1000000';

    close = document.createElement('a');
    close.href = 'javascript:window._____pshb_closeMe();';
    close.innerHTML = '&times;';
    s = close.style;
    s.cursor = 'default';
    s.fontWeight = 'bold';
    s.fontSize = '12px';
    s.position = 'absolute';
    s.top = '15px';
    s.right = '15px';
    s.margin = '0';
    s.borderStyle = 'dotted';
    s.borderColor = '#aaa;';
    s.borderWidth = '0 0 1px 1px';
    s.padding = '0 3px 0 3px';
    s.display = 'block';
    s.textDecoration = 'none';
    s.color = '#000';
    s.zIndex = '1000001';
  };

  // Thanks Prototype.
  var canonicalize = function(s) {
    if (s == '' || s == null) {
      return '';
    };
    var temp = document.createElement('div');
    temp.innerHTML = s.toLowerCase();
    var result = temp.childNodes[0].nodeValue;
    temp.removeChild(temp.firstChild);  // garbage collection
    return result;
  };

  window._____pshb_closeMe = function() {
    document.body.removeChild(close);
    document.body.removeChild(post);
  };

  var findAtomFeed = function() {
    var links = document.getElementsByTagName('link');
    for (var i = 0; i < links.length; ++i) {
      var item = links[i];
      if (item.type != undefined &&
          item.href != undefined &&
          item.rel != undefined &&
          canonicalize(item.type).indexOf('application/atom') == 0 &&
          canonicalize(item.rel).indexOf('alternate') == 0 &&
          item.href.length > 0) {
        return item.href;
      };
    };
    return null;
  };

  window._____pshb_sawLoad = false;
  window._____pshb_autoClose = true;

  // TODO: Figure out a better way to detect event delivery completion.
  // XHR can see 204 responses but doing this cross-domain seems impossible.
  // Proxying the publish POST through a server because that will hide the IP
  // address of the requestor; this leaves publishing open to a DoS attack,
  // which we want to avoid.
  window._____pshb_handleLoad = function() {
    if (!window._____pshb_sawLoad) {
      window._____pshb_sawLoad = true;
    } else {
      // This means the iframe has loaded another page, which could not possibly
      // be a 204 response (since browsers do nothing on that response). So here
      // we assume the post failed and do not automatically close the window.
      window._____pshb_autoClose = false;
    };
  };

  var autoClose = function() {
    if (window._____pshb_autoClose) {
      window._____pshb_closeMe();
    };
  };


  if (post != null) {
    var feed = findAtomFeed();
    var hub = _____pshb_getHub();
    post.onload = _____pshb_handleLoad;
    // Remove the domain portion of the URL below for local testing.
    post.src = 'http://pubsubhubbub.appspot.com/bookmarklet.html' + '?feed=' + feed + '&hub=' + hub;
    document.body.appendChild(post);
    document.body.appendChild(close);
    setTimeout(autoClose, 2000);
  };
};

_____pshb_BookmarkletRun();
