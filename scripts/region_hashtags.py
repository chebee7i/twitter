"""
Print the top 5000 hashtags for a given region.

"""

import twitterproj

db = twitterproj.connect()
hashtags = twitterproj.sorted_hashtags(bot_filtered=True)

# Beaver County, UT
region = db.grids.counties.bot_filtered.find({'geoid': "49001"}).next()

# Note that the bot_filtered nature of `hashtags` and `region` should agree.
print(twitterproj.region_hashtags_str(region, hashtags, None))
