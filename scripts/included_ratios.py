"""
For each region, calculate the ratio of:

    the total number of hashtag mentions for hashtags appearing in the local
    region's hashtags top N (ranked by local hashtag frequency) that also
    appear in the global top N hashtags (ranks by global hashtag frequency)

to

    the total number of hashtag mentions for hashtags appearing in the local
    region's hashtags top N (ranked by local hashtag frequency)

Note: N is the same locally and globally in this example.


"""
from __future__ import print_function

import twitterproj
import json

db = twitterproj.connect()
collections = [(db.grids.squares.bot_filtered, '_squares', '_id'),
               (db.grids.counties.bot_filtered, '_counties', 'geoid'),
               (db.grids.states.bot_filtered, '_states', 'fips')]
hashtags = twitterproj.sorted_hashtags(bot_filtered=True)

N = 5000
hashtags = hashtags[:N]

func = twitterproj.included_ratio
for collection, suffix, key in collections:
    ratios = {}
    for region in collection.find():
        print(region[key])
        ratio, tagged = func(region['counts'], hashtags, topN=N)
        ratios[region[key]] = ratio

    with open('top5000ratios' + suffix + '.json', 'w') as f:
        f.write(json.dumps(ratios))
