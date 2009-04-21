// Copyright 2009 Google Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//      http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

(function() {
  if (document.body && !document.xmlVersion) {
    var jsonp = document.createElement('script');
    jsonp.type = 'text/javascript';
    // Remove the domain portion of the URL below for local testing.
    jsonp.src = 'http://pubsubhubbub.appspot.com/bookmarklet_jsonp.min.js?rand=' + Math.floor(Math.random() * 1000);
    var head = document.getElementsByTagName('head')[0].appendChild(jsonp);
  };
})()
