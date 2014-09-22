"""
Build hashtag counts for counties with and without bot filtration.

Takes about 1 hour per collection.

"""
import twitterproj as t

def build_counties_collection(bot_filtered=True, dry_run=True):
    db = t.connect()
    if bot_filtered:
        skip = t.subcollections.get_skip_users()
        county_coll = db.grids.counties.bot_filtered
    else:
        skip = None
        county_coll = db.grids.counties

    t.subcollections.build_hashtag_counts_by_county(
        db.tweets.with_hashtags,
        county_coll,
        '../tiger/tl_2014_us_county.shp',
        skip_users=skip,
        dry_run=dry_run
    )

if __name__ == '__main__':
    #build_counties_collection(bot_filtered=True, dry_run=False)
    #build_counties_collection(bot_filtered=False, dry_run=False)
    pass

