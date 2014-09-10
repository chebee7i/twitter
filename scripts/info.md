Geotagged tweets from the streaming API were collected. Data collected from 
2014-04-25 to 2014-09-01 were filtered and added to a mongodb database,
subject to the following constraint:

    - tweet must have a GPS location within the bbox of the continential usa
      [-124.7625, 24.5210, -66.9326, 49.3845]
      This excludes locations derived from a manually specified "place".
      See `location_match` for more details.


The number of continential usa tweets with GPS coordinates and hashtags:

    > db.tweets.find({'hashtags.0': {$exists: 1}}).count()
    39610005

The total number of continential usa tweets with GPS coordiantes:

    > db.tweets.count()
    345200158
