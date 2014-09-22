"""
Build hashtag counts for states with bot filtration.

Takes about 1 hour per collection.

"""
import twitterproj as t

def build_states_collection(bot_filtered=True, dry_run=True):
    db = t.connect()
    if bot_filtered:
        skip = t.subcollections.get_skip_users()
        state_coll = db.grids.states.bot_filtered
    else:
        skip = None
        state_coll = db.grids.states

    t.subcollections.build_hashtag_counts_by_state(
        db.tweets.with_hashtags,
        state_coll,
        '../tiger/tl_2014_us_state.shp',
        skip_users=skip,
        dry_run=dry_run
    )

if __name__ == '__main__':
    build_states_collection(bot_filtered=True, dry_run=True)
    #build_states_collection(bot_filtered=False, dry_run=True)
    pass

