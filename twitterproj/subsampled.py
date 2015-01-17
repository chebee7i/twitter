"""
Subsampling the data to combat oversampling high population areas.

"""
from __future__ import division

from collections import defaultdict, OrderedDict
import dit
import numpy as np
import us

import twitterproj

def county_userdist(county_geoid, style, count='numTweets', db=None):
    if db is None:
        db = twitterproj.connect()
    collection = db.grids.userstats.counties.bot_filtered
    usercounts = defaultdict(int)
    for countyuser in collection.find({'geoid': county_geoid}):
        usercounts[countyuser['user_id']] += countyuser[count]

    users = list(usercounts.keys())
    if len(users) == 0:
        return None

    if style == 'uniform':
        pmf = np.array([1/len(users)] * len(users))
    elif style == 'count':
        # Proportional to user tweet count
        pmf = np.array(list(usercounts.values()), dtype=float)
        pmf /= pmf.sum()
    else:
        raise Exception('Bad distribution type.')
    d = dit.ScalarDistribution(users, pmf, sort=False, base='linear', sparse=False)
    return d

def sample_users(nUsers, dist):
    """
    Implement sampling without replacement.

    """
    d = dist.copy()
    d.make_dense()

    # Make sure repeated calls give new users.
    d.prng = np.random.RandomState()

    if nUsers > len(d):
        msg = 'Not enough outcomes to draw {0} samples without replacement.'
        raise Exception(msg.format(nUsers))

    users = []
    while len(users) < nUsers:
        user = d.rand(1)[0]
        d[user] = 0
        d.normalize()
        users.append(user)

    return set(users)

def subsampled_hashtag_counts__counties(geoid, user_ids, db=None):
    if db is None:
        db = twitterproj.connect()

    collection = db.grids.userstats.counties.bot_filtered

    hashtag_counts = defaultdict(int)
    for countyuser in collection.find({'geoid': geoid}):
        if countyuser['user_id'] in user_ids:
            for hashtag in user['hashtags']:
                hashtag_counts[hashtag] += 1

    return hashtag_counts

def userentropy__counties(count='numTweets', style='count', normalized=False, db=None):
    """
    Returns entropy of distribution: (# tweets by user) / (# tweets in county).

    """
    if db is None:
        db = twitterproj.connect()

    desired = [s.fips for s in us.states.STATES_CONTIGUOUS]
    counties = twitterproj.counties_from_json()

    entropies = OrderedDict()
    for state_fips in counties:
        for county in counties[state_fips]:
            geoid = county['GEOID']
            d = county_userdist(geoid, style, count=count)
            if d is None:
                continue
            h = dit.shannon.entropy(d)
            if normalized:
                h /= d.ops.log(len(d))
            entropies[geoid] = h

    return entropies
