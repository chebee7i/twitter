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

import twitterproj

if __name__ == '__main__':
    tweets, db, conn = twitterproj.connect()

    coords = [
        [
            [-122.4596959,47.4810022],
            [-122.4596959,47.7341388],
            [-122.2244329,47.7341388],
            [-122.2244329,47.4810022],
            [-122.4596959,47.4810022]
        ]
    ]
    counts, skipped = twitterproj.polygon_hashtags(db.hashtagTweets, coords)
