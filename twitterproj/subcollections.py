"""
Functions used to create/query subcollections.

"""

from collections import OrderedDict

import pymongo
import fiona
import json
import us

from .geo import hashtag_counts_in

def build_hashtag_counts_by_state(collection, shpfile, dry_run=True):
    # ../tiger/tl_2014_us_state.shp
    hci = hashtag_counts_in

    state_counts = collection.database.grids.states
    if not dry_run:
        state_counts.drop()
    with fiona.open(shpfile, 'r') as f:
        out = {}
        for i, feature in enumerate(f):
            doc = OrderedDict()

            name = feature['properties']['NAME']
            print(name)
            geometry = feature['geometry']
            counts, skipped = hci(collection, geometry)

            doc = OrderedDict()
            doc['name'] = feature['properties']['NAME']
            doc['counts'] = counts
            doc['fips'] = feature['properties']['STATEFP']
            doc['abbrev'] = feature['properties']['STUSPS']

            if dry_run:
                continue

            try:
                state_counts.insert(doc)
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
                state_counts.insert(doc)
                state_counts.insert(doc2)

def hashtag_counts_by_state(key, val, db):
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
    c = db.grids.states.find({key: val})
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

def hashtag_counts__states(db):
    """
    Generator of hashtag counts for each contiguous "state".

    """
    states = us.STATES
    avoid = set(['AK', 'HI'])
    for state in states:
        if state.abbr in avoid:
            continue
        else:
            yield hashtag_counts_by_state('abbrev', state.abbr, db)
