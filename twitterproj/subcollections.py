"""
Functions used to create/query subcollections.

"""
from __future__ import print_function

from collections import OrderedDict, defaultdict

import pymongo
import fiona
import json
import us
import os

from .geo import hashtag_counts_in
from .helpers import connect

__all__ = [
    'hashtag_counts__states',
    'hashtag_counts__counties',
    'hashtag_counts__squares',
    'counties_from_json',
    'get_hashtag_counts',
]

def counties_from_json(filename=None):
    """
    Extract county information by state FIPS code from JSON file.

    """
    if filename is None:
        # Ugly hack for now.
        # Will only if site-packages directory is symlinked to the repository.
        path = os.path.realpath(__file__)
        basedir, filename = os.path.split(path)
        filename = os.path.join(basedir, '../tiger/tl_2014_us_county_properties.json')

    # Perhaps helpful:
    # https://www.census.gov/geo/reference/lsad.html

    with open(filename) as f:
        counties = json.load(f, object_pairs_hook=OrderedDict)
    return counties

def write_counties_json(jsonfilename, shpfilename):
    """
    Writes a JSON file containing county information.

    All TIGER/Line property information is extracted and sorted by the state
    FIPS code.

    """
    counties = defaultdict(list)
    with fiona.open(shpfilename) as f:
        for feature in f:
            properties = feature['properties']
            counties[properties['STATEFP']].append(properties)

    counties = counties.items()
    counties.sort()
    counties = OrderedDict(counties)
    with open(jsonfilename, 'w') as f:
        json.dump(counties, f)

def get_skip_users():
    db = connect()
    coll = db.users.flagged
    uids = [u['_id'] for u in coll.find({'avoid': True}, {'_id': True})]
    return uids

def build_hashtag_counts_by_county(tweet_collection, county_collection, shpfile, skip_users=None, dry_run=True):
    """

    """
    # ../tiger/tl_2014_us_county.shp
    hci = hashtag_counts_in

    all_skipped = 0
    skips = {}

    fips = set([state.fips for state in us.STATES_CONTIGUOUS])

    if not dry_run:
        county_collection.drop()
    with fiona.open(shpfile, 'r') as f:
        out = {}
        for i, feature in enumerate(f):
            doc = OrderedDict()
            state_fips = feature['properties']['STATEFP']
            if state_fips not in fips:
                # Consider only counties in the contiguous US.
                continue
            name = feature['properties']['NAMELSAD']
            msg = u"{0}, {1}".format(name, fips[state_fips])
            print(msg.encode('utf-8'))

            geometry = feature['geometry']
            counts, skipped = hci(tweet_collection, geometry, skip_users)
            print("\tSkipped {0} tweets due to user ids.".format(skipped))
            all_skipped += skipped
            skips[name] = skipped
            doc = OrderedDict()
            doc['name'] = name
            doc['counts'] = counts
            doc['state_fips'] = state_fips
            doc['county_fips'] = feature['properties']['COUNTYFP']
            doc['geoid'] = feature['properties']['GEOID']
            doc['landarea'] = feature['properties']['ALAND']
            if dry_run:
                continue

            try:
                county_collection.insert(doc)
            except pymongo.errors.DocumentTooLarge:
                # Hack for too large...probably won't happen.
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
                county_collection.insert(doc)
                county_collection.insert(doc2)

    msg = "\nIn total, skipped {0} tweets due to user ids"
    print(msg.format(all_skipped))
    return skips



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

    all_skipped = 0
    skips = {}

    # The abbreviations of the contiguous states
    desired = us.states.mapping('abbr', 'fips', us.STATES_CONTIGUOUS)

    if not dry_run:
        state_collection.drop()
    with fiona.open(shpfile, 'r') as f:
        out = {}
        for i, feature in enumerate(f):
            doc = OrderedDict()
            if feature['properties']['STATEFP'] not in desired:
                continue

            name = feature['properties']['NAME']
            print(name)
            geometry = feature['geometry']
            counts, skipped = hci(tweet_collection, geometry, skip_users)
            print("\tSkipped {0} tweets due to user ids.".format(skipped))
            all_skipped += skipped
            skips[name] = skipped
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

    msg = "\nIn total, skipped {0} tweets due to user ids"
    print(msg.format(all_skipped))
    return skips

def get_hashtag_counts(key, val, collection):
    """
    Returns a dict of the region from the colletion.

    For states, you receive a document containing keys:

        'name', 'fips', 'counts', 'abbrev', 'landarea'

    The value associated to 'counts' is a dict of hashtag and hashtag counts.
    Query by: 'name', 'fips', or 'abbrev'.

    For counties, you receive a document containing keys:

        'name', 'state_fips', 'county_fips', 'geoid', 'landarea'

    Etc.


    Examples:

        hashtag_counts_by_state('name', 'Washington', collection)
        hashtag_counts_by_state('fips', 53, collection)
        hashtag_counts_by_state('abbrev', 'WA', collection)

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
    states = us.STATES_CONTIGUOUS
    for state in states:
        yield get_hashtag_counts('abbrev', state.abbr, collection)

def hashtag_counts__counties(db, state=None, bot_filtered=True,
                              county_json=None):
    """
    Generator of hashtag counts for each county in the contiguous states.

    Optionally, filter by state abbreviation.

    """
    if bot_filtered:
        collection = db.grids.counties.bot_filtered
    else:
        collection = db.grids.counties

    # Unlike the states collection, the db only contains counties that
    # are in the lower 48+1 states.
    states = us.STATES_CONTIGUOUS
    counties = counties_from_json()
    fips_lookup = us.states.mapping('abbr', 'fips', states)

    if state is not None:
        desired = state
        states = [state for state in states if state.abbr == desired]

    # Get a list of the counties!
    for state in states:
        for county in counties[state.fips]:
            yield get_hashtag_counts('geoid', county['GEOID'], collection)

def hashtag_counts__squares(db, bot_filtered=True):
    """
    Generator of hashtag counts for each square.

    """
    if bot_filtered:
        collection = db.grids.squares.bot_filtered
    else:
        collection = db.grids.squares

    # No need to use get_hashtag_counts since all counts for each square
    # fit in a single document.
    ids = [d['_id'] for d in collection.find({}, {'_id': True})]
    ids.sort()
    for _id in ids:
        yield collection.find({'_id': _id}).next()


