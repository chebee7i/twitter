"""
Functions used to create/query subcollections.

"""

from collections import OrderedDict

import pymongo
import fiona
import json
import us

from .geo import hashtag_counts_in
from .helpers import connect

__all__ = [
    'hashtags_counts__states',
]

def get_skip_users():
    db = connect()
    coll = db.users.flagged
    uids = [u['_id'] for u in coll.find({'avoid': True}, {'_id': True})]


def build_hashtag_counts_by_state(tweet_collection, state_collection, shpfile,
                                  skip_users=None, dry_run=True):
    """
    Example
    -------

    Build state counts using hashtagged tweets and skip bots.
    >>> skip = get_skip_users()
    >>> build_hashtag_counts_by_state(db.tweets.with_hashtags,
    ...                               db.grids.states.bot_filtered,
    ...                               '../tiger/tl_2014_us_state.shp',
    ...                               skip_users=skip)
    ...

    Build state counts using hashtagged tweets and do not skip bots.
    >>> build_hashtag_counts_by_state(db.tweets.with_hashtags,
    ...                               db.grids.states,
    ...                               '../tiger/tl_2014_us_state.shp')
    ...

    """

    # ../tiger/tl_2014_us_state.shp
    hci = hashtag_counts_in

    if not dry_run:
        state_collection.drop()
    with fiona.open(shpfile, 'r') as f:
        out = {}
        for i, feature in enumerate(f):
            doc = OrderedDict()

            name = feature['properties']['NAME']
            print(name)
            geometry = feature['geometry']
            counts, skipped = hci(tweet_collection, geometry, skip_users)
            print("\tSkipped {0} tweets due to user ids.".format(skipped))
            doc = OrderedDict()
            doc['name'] = feature['properties']['NAME']
            doc['counts'] = counts
            doc['fips'] = feature['properties']['STATEFP']
            doc['abbrev'] = feature['properties']['STUSPS']
            doc['landarea'] = feature['properties']['ALAND']
            if dry_run:
                continue

            try:
                state_collection.insert(doc)
            except pymongo.errors.DocumentTooLarge:
                # Hack for CA.
                # Split in two...must be careful to join when querying.
                del doc['counts']
                del doc['_id']
                doc2 = doc.copy()
                items = counts.items()
                L = len(items)/2
                counts1 = dict(items[:L])
                counts2 = dict(items[L:])
                doc['counts'] = counts1
                doc2['counts'] = counts2
                state_collection.insert(doc)
                state_collection.insert(doc2)

def hashtag_counts_by_state(key, val, collection):
    """
    Returns a dict of the state containing keys:

        'name', 'fips', 'counts', 'abbrev'.

    The value associated to 'counts' is a dict of hashtag and hashtag counts.

    Query by: 'name', 'fips', or 'abbrev'.

    Examples:

        hashtag_counts_by_state('name', 'Washington')
        hashtag_counts_by_state('fips', 53)
        hashtag_counts_by_state('abbrev', 'WA')

    """
    c = collection.find({key: val})
    hashtag_counts = {}
    first = True
    for doc in c:
        if first:
            hashtag_counts.update(doc)
            first = False
        else:
            # After the first one, we must explictly update the inner dict.
            hashtag_counts['counts'].update(doc['counts'])
    return hashtag_counts

def hashtag_counts__states(db, bot_filtered=True):
    """
    Generator of hashtag counts for each contiguous "state".

    """
    if bot_filtered:
        collection = db.grids.states.bot_filtered
    else:
        collection = db.grids.states
    states = us.STATES
    avoid = set(['AK', 'HI'])
    for state in states:
        if state.abbr in avoid:
            continue
        else:
            yield hashtag_counts_by_state('abbrev', state.abbr, collection)
