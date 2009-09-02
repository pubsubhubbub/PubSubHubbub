spec: pubsubhubbub-core-0.2.html

pubsubhubbub-core-0.2.html: pubsubhubbub-core-0.2.xml
	xml2rfc pubsubhubbub-core-0.2.xml pubsubhubbub-core-0.2.html
	cat pubsubhubbub-core-0.2.html | head -n -1 > pubsubhubbub-core-0.2.html
	cat analytics.txt >> pubsubhubbub-core-0.2.html

