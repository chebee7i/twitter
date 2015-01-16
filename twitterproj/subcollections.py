"""
Functions used to create/query subcollections.

"""
from __future__ import print_function

from collections import OrderedDict, defaultdict

import pymongo
import fiona
import json
from bson import json_util
import us
import os
import sys

from .geo import hashtag_counts_in, tweets_in_region
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

    Still used by: hashtag_counts__county

    """
    if filename is None:
        # Ugly hack for now.
        # Will work only if site-packages directory is symlinked to the repository.
        path = os.path.realpath(__file__)
        basedir, filename = os.path.split(path)
        filename = os.path.join(basedir, '../tiger/tl_2014_us_county_properties.json')

    # Perhaps helpful:
    # https://www.census.gov/geo/reference/lsad.html

    with open(filename) as f:
        counties = json.load(f, object_pairs_hook=OrderedDict)

    # Now sort the counties within each state
    def key_func(x):
        return x['GEOID']
    for state_fips in counties.keys():
        c = counties[state_fips]
        counties[state_fips] = sorted(c, key=key_func)
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
            doc['geometry'] = geometry
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
                L = int(len(items)/2)
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
    desired = us.states.mapping('fips', 'abbr', us.STATES_CONTIGUOUS)
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
            doc['geometry'] = geometry
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
                L = int(len(items)/2)
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

def hashtag_counts__states(db=None, bot_filtered=True):
    """
    Generator of hashtag counts for each contiguous "state".

    """
    if db is None:
        db = connect()
    if bot_filtered:
        collection = db.grids.states.bot_filtered
    else:
        collection = db.grids.states
    states = us.STATES_CONTIGUOUS # sorted alphabetically
    for state in states:
        yield get_hashtag_counts('abbrev', state.abbr, collection)

def hashtag_counts__counties(db=None, state=None, bot_filtered=True,
                              county_json=None):
    """
    Generator of hashtag counts for each county in the contiguous states.

    Optionally, filter by state abbreviation.

    """
    if db is None:
        db = connect()
    if bot_filtered:
        collection = db.grids.counties.bot_filtered
    else:
        collection = db.grids.counties

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

def hashtag_counts__squares(db=None, bot_filtered=True):
    """
    Generator of hashtag counts for each square.

    """
    if db is None:
        db = connect()
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

def build_userstats_by_county(tweet_collection, county_collection, shpfile,
                              skip_users=None, dry_run=True, mod_filter=None):
    """
    Builds a collection of user stats within each county.
    All tweets are partitioned by county.

    The created collection will consist of documents with the following
    information, which applies per user within the region.

        state_fips : The state fips of the county
        county_fips : The county fips
        geoid : The state-county fips code
        user_id : The user id
        numHashtags : Total number of hashtags that were tweeted
        numHashtagsUnique : Number of distinct hashtags that were tweeted
        hashtags : List of unordered, tweeted hashtags, with repeats
        numTweets : Total number of tweets
        numTweetsWithHashtags: Total number of tweets with hashtags

    The following correspond to to each other, but they are not
    chronologically ordered. The should simply be extended by
    other counts from user tweets in other regions. Using the statuses
    count may allow one to order the elements.

        followers_count : The number of followers at each tweet
        friends_count : The number of friends at each tweet
        favourites_count: The number of favourited statuses at each tweet
        statuses_count: The number of statuses at each tweet
        create_at: The creation date and time of each tweet

    Note that the statuses_count can be used to get a better sense of how many
    tweets we have missed from the users. This total number combined with the
    ratio of numTweetsWithHashtags/numTweets can be used to estimate the number
    of tweets with hashtags that we missed, with some assumptions about a fixed
    ratio of geotagged tweets from the user.

    Parameters
    ----------
    tweet_collection :
        The collection containing the tweets to partition.
    county_collection :
        This is where the user stats will be stored.
        It will be emptied if dry_run is False.
    shpfile :
        This is the shapefile that contains county information.
        In particular, it must contain the geometry of the county.
    skip_users :
        The set of user ids to skip.
        For the current db, this is an iterable of ints.
        For the future, it should be an iterable of strings,
        and then we will query id_str, instead of id.
    dry_run : bool
        If True, then we only extract and build the stats, but nothing
        is saved to the collection.

    Returns
    -------
    skips : dict
        The total number of skipped tweets in each region by name.
        This is mostly informational. The name is not a useful key.

    """
    # ../tiger/tl_2014_us_county.shp
    hci = hashtag_counts_in

    all_skipped = 0
    skips = {}

    fips = set([state.fips for state in us.STATES_CONTIGUOUS])
    mapping = us.states.mapping('fips', 'name')

#    if not dry_run:
#        county_collection.drop()

    with fiona.open(shpfile, 'r') as f:
        for i, feature in enumerate(f):

            if mod_filter is not None and i % 10 != mod_filter:
                continue

            # This holds the documents for each user in the region.
            docs = defaultdict(OrderedDict)

            # Content common to every document.
            state_fips = feature['properties']['STATEFP']
            county_fips = feature['properties']['COUNTYFP']
            geoid = feature['properties']['GEOID']

            if state_fips not in fips:
                # Consider only counties in the contiguous US.
                continue

            # Log info...
            name = feature['properties']['NAMELSAD']
            msg = u"{0}:\t{1}, {2}, {3}".format(i,
                                                name,
                                                mapping[state_fips],
                                                geoid)
            print(msg.encode('utf-8'))
            sys.stdout.flush()

            def process_tweet(tweet):
                # Update to id_str eventually.
                user_id = tweet['user']['id']
                if user_id not in skip_users:
                    # Store data.
                    if user_id not in docs:
                        doc = docs[user_id]
                        doc['state_fips'] = state_fips
                        doc['county_fips'] = county_fips
                        doc['geoid'] = geoid
                        doc['user_id'] = user_id
                        # Prep collection
                        doc['numHashtags'] = 0
                        doc['numHashtagsUnique'] = 0
                        doc['hashtags'] = []
                        doc['numTweets'] = 0
                        doc['numTweetsWithHashtags'] = 0
                        doc['followers_count'] = []
                        doc['friends_count'] = []
                        doc['favourites_count'] = []
                        doc['statuses_count'] = []
                        doc['created_at'] = []

                    doc = docs[user_id]
                    doc['numTweets'] += 1
                    doc['hashtags'].extend(tweet['hashtags'])
                    if len(tweet['hashtags']):
                        doc['numTweetsWithHashtags'] += 1
                    doc['followers_count'].append(tweet['user']['followers_count'])
                    doc['friends_count'].append(tweet['user']['friends_count'])
                    doc['favourites_count'].append(tweet['user']['favourites_count'])
                    doc['statuses_count'].append(tweet['user']['statuses_count'])
                    doc['created_at'].append(tweet['created_at'])

            geometry = feature['geometry']
            try:
                # This becomes prohibitively slow when the collection is large.
                raise Exception


                # Fetch all tweets in the region
                tweets = tweets_in_region(tweet_collection, geometry)
                for tweet in tweets.sort('created_at', 1):
                    process_tweet(tweet)
                do_sort = False
            except (pymongo.errors.OperationFailure, Exception):
                # Fetch all tweets in the region
                print("Failed to sort in mongo. Manually sorting")
                sys.stdout.flush()
                tweets = tweets_in_region(tweet_collection, geometry)
                do_sort = True
                for tweet in tweets:
                    process_tweet(tweet)

            # The sort, to be efficient and use the index requires that the
            # created_at be listed before the 2dspatial.
            # See: http://blog.mongolab.com/2012/06/cardinal-ins/

            # Finalize docs
            for doc in docs.values():
                doc['numHashtags'] = len(doc['hashtags'])
                doc['numHashtagsUnique'] = len(set(doc['hashtags']))

                # Now sort by datetime.
                if do_sort:
                    z = sorted(zip(doc['created_at'],
                                   doc['statuses_count'],
                                   doc['followers_count'],
                                   doc['friends_count'],
                                   doc['favourites_count']))
                    z = list(zip(*z))
                    doc['created_at'] = z[0]
                    doc['statuses_count'] = z[1]
                    doc['followers_count'] = z[2]
                    doc['friends_count'] = z[3]
                    doc['favourites_count'] = z[4]

            print("\tDone finalizing")
            sys.stdout.flush()
            if dry_run:
                continue

            try:
                county_collection.insert(docs.values())
            except pymongo.errors.DocumentTooLarge:
                # This shouldn't happen. Fail hard.
                raise



