"""
Example scripts for spatial queries.

db.hashtagTweets.find({coordinates: {$geoWithin : { $geometry : {type:
    "Polygon", coordinates: [
        [[-122.4596959,47.4810022],
         [-122.4596959,47.7341388],
         [-122.2244329,47.7341388],
         [-122.2244329,47.4810022],
         [-122.4596959,47.4810022]]
    ] }}}})


"""
from __future__ import print_function

from collections import OrderedDict

import pymongo
import twitterproj
import fiona
import json

def seattle_counts(collection):
    seattle = [
        [
            [-122.4596959,47.4810022],
            [-122.4596959,47.7341388],
            [-122.2244329,47.7341388],
            [-122.2244329,47.4810022],
            [-122.4596959,47.4810022]
        ]
    ]
    counts, skipped = twitterproj.hashtag_counts_in(collection, seattle)
    return counts, skipped

if __name__ == '__main__':
    db = twitterproj.connect()
    #counts, skipped = seattle_counts(db.tweets.with_hashtags)
    counts = twitterproj.hashtag_counts_by_state('abbrev', 'WA', db)

