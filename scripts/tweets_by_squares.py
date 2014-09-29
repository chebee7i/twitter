"""
Throwaway function to get user counts per square.

"""
from __future__ import print_function

from collections import OrderedDict, defaultdict

import pymongo
import os

import twitterproj as t

def main():
    # Output will be dictionary relating
    #    id for the square
    # to dictionary of user ids to counts

    db = t.connect()
    squares = OrderedDict()
    coll = db.grids.squares.bot_filtered
    for square in coll.find().sort('_id', pymongo.ASCENDING):
        squares[square['_id']] = square['geometry']

    collection = db.tweets.with_hashtags
    out = OrderedDict()
    skip_users = t.subcollections.get_skip_users()
    for sq_id, geometry in squares.items():
        print("Fetching counts for square {0}".format(sq_id))
        counts = t.user_counts_in(collection, geometry, skip_users)
        # Convert counts from a defaultdict to a dict
        out[sq_id] = (dict(counts[0]), counts[1])

    return out

def n_unique(counts):
    squares = {}
    for sqid, (users, skipped) in counts.items():
        squares[sqid] = len(users)
    return squares

def to_csv(counts, filename):
    with open(filename, 'w') as f:
        f.write(','.join(map(str, n_unique(counts).values())))

if __name__ == '__main__':
    counts = main()
