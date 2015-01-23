"""
Build userstats for counties with and without bot filtration.

Takes about X hour per collection.

"""
import twitterproj as t

def build_userstats_by_county(bot_filtered=True, dry_run=True, mod_filter=None):
    db = t.connect()
    if bot_filtered:
        skip = t.subcollections.get_skip_users()
        county_coll = db.grids.userstats_new.counties.bot_filtered
    else:
        skip = None
        county_coll = db.grids.userstats.counties

    t.subcollections.build_userstats_by_county(
        db.tweets, # All tweets not just those with hashtags
        county_coll,
        '../tiger/tl_2014_us_county.shp',
        skip_users=skip,
        dry_run=dry_run,
        mod_filter=mod_filter
    )

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 2:
        mod_filter = int(sys.argv[-1])
    else:
        mod_filter = None

    build_userstats_by_county(bot_filtered=True,
                              dry_run=False,
                              mod_filter=mod_filter)

