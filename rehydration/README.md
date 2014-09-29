Tweet Rehydration
=================

Various applications for interacting with the Twitter API and manipulating tweets. A few:

Twitter Terms of Service notes
------------------------------

Due to Twitter terms of service limitations you cannot re-distribute tweets, but only tweet
identifiers. Users must use the Twitter API to re-hydrate the tweets using the ids. This allows
Twitter to delete or otherwise restrict tweets if necessary. Also, twitter does not allow
applications to support programmatic access to twitter data but only a "bulk download" capability
that is limited to 100k tweets per day.

Twitter Rules (https://dev.twitter.com/overview/terms/rules-of-the-road)
See section I.4.1 (limit on downloadable datasets, redistribution)

Twitter Terms of Service (https://twitter.com/tos)
See section 8
"...you have to use the Twitter API if you want to reproduce, modify, create derivative works,
distribute, sell, transfer, publicly display, publicly perform, transmit, or otherwise use the
Content or Services."

Twitter API documentation (https://dev.twitter.com/rest/public)

Table of Contents
-----------------

* rehydrate_tweets.pl - Given a list of tweet ids rehydrate the twitter objects and save as a list of json objects.

