bookmarklet: bookmarklet.min.js bookmarklet_jsonp.min.js

%.min.js: %.js
	cat $^ | python jsmin.py | tail -n 1 > $@
