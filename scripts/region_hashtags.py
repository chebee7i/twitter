"""
Print the top 5000 hashtags for a given region.

"""
from __future__ import unicode_literals
from __future__ import print_function

import twitterproj

db = twitterproj.connect()
hashtags = twitterproj.sorted_hashtags(bot_filtered=True)

# Let's see what hashtags from Beaver County, UT are in the top 5000.
hashtags = hashtags[:5000]

# Beaver County, UT
region = db.grids.counties.bot_filtered.find({'geoid': "49001"}).next()
counts = twitterproj.region_sorted_hashtags(region)

# Note that the bot_filtered nature of `hashtags` and `region` should agree.
ratio, tagged = twitterproj.included_ratio(counts, hashtags)

for hashtag, (count, included) in tagged.items():
    print("{},{},{}".format(hashtag, count, included))
