bookmarklet: bookmarklet.min.js bookmarklet_jsonp.min.js bookmarklet_config.html

%.min.js: %.js
	cat $^ | python jsmin.py | tail -n 1 > $@

bookmarklet_config.html: bookmarklet_config.template bookmarklet.min.js
	python generate_config.py > $@
