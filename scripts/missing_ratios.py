"""
For each region, calculate the ratio of:

    the total number of hashtag mentions for hashtags appearing in the local
    region's top N hashtags (ranked by local hashtag frequency) that do not
    appear in the global top N hashtags (ranks by global hashtag frequency)

to

    the total number of hashtag mentions for hashtags appearing in the local
    region's top N hashtags (ranked by local hashtag frequency)

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
func = twitterproj.region_missing_tophashtags
for collection, suffix, key in collections:
    ratios = {}
    for region in collection.find():
        print(region[key])
        tlmissing, tl = func(region, hashtags, N, N)
        ratio = twitterproj.missing_ratio(tlmissing, tl)
        ratios[region[key]] = ratio

    with open('top5000ratios' + suffix + '.json', 'w') as f:
        f.write(json.dumps(ratios))
