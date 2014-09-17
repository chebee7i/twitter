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

import twitterproj
import fiona

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

def hashtag_counts_by_state(collection, shpfile):
    hci = twitterproj.hashtag_counts_in

    with fiona.open(shpfile, 'r') as f:
        out = {}
        for feature in f:
            name = feature['properties']['NAME']
            print(name)
            geometry = feature['geometry']
            counts, skipped = hci(collection, geometry)
            out[name] = counts
    return out


if __name__ == '__main__':
    db = twitterproj.connect()
    #counts, skipped = seattle_counts(db.hashtagTweets)
    counts = hashtag_counts_by_state(db.hashtagTweets,
                                     '../tiger/tl_2014_us_state.shp')
