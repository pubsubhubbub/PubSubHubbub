#!/usr/bin/env python

js = open('bookmarklet.min.js').read()
config = open('bookmarklet_config.template').read()
print config % {'bookmarklet_min_js': js}
