"""
Insert the number of users that tweeted each hashtag.

"""
import twitterproj
import pymongo
from collections import defaultdict
import itertools
import json
import os
import io

db = twitterproj.connect()

def add_user_counts(bot_filtered=True):
    collection = db.tweets.with_hashtags
    if bot_filtered:
        skip_users = twitterproj.subcollections.get_skip_users()
        target = db.hashtags.bot_filtered
    else:
        skip_users = set([])
        target = db.hashtags

    counts = defaultdict(int)
    users = defaultdict(set)
    for i, tweet in enumerate(collection.find()):
        user_id = tweet['user']['id']
        if user_id in skip_users:
            continue

        for hashtag in tweet['hashtags']:
            counts[hashtag] += 1
            users[hashtag].add(user_id)

    for i, (hashtag, count) in enumerate(counts.iteritems()):
        target.update({'_id': hashtag, 'count': count},
                      {"$set": {'user_count': len(users[hashtag])}},
                      upsert=False)

def to_json(filename, mincount=1000, bot_filtered=True):

    if bot_filtered:
        collection = db.hashtags.bot_filtered
    else:
        collection = db.hashtags

    rows = []
    if mincount is not None:
        it = collection.find({'user_count': {'$gte': mincount}})
    else:
        it = colelction.find()
    for doc in it:
        row = [doc['_id'], doc['count'], doc['user_count']]
        rows.append(row)

    data = {'data': rows}
    with open(filename, 'w') as fobj:
        json.dump(data, fobj)


def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

def to_csv(filename, mincount=1000, bot_filtered=True):
    """
    Writes hashtags to CSV, filtering hashtags that were not mentioned by
    some minimum number of users.

    """
    if bot_filtered:
        collection = db.hashtags.bot_filtered
    else:
        collection = db.hashtags

    rows = []
    if mincount is not None:
        it = collection.find({'user_count': {'$gte': mincount}})
    else:
        it = colelction.find()
    it = it.sort('user_count', pymongo.DESCENDING)

    basename, ext = os.path.splitext(filename)
    if not ext:
        ext = '.csv'

    data = """
This file contains information regarding the UTF-8 encoded CSV file:

    {0}{1}

Each line of that file contains 3 pieces of information, separated by commas:

    1. hashtag
    2. number of times the hashtag was tweeted
    3. number of users who tweeted the hashtag

Lines are sorted, descendingly, according to column 3.
Counts are tabulated wrt geotagged tweets in the contiguous states.
{2}
Hashtags were included only if the were tweeted by at least {3} users.
"""
    if bot_filtered:
        text = 'Tweets from users determined to be robots were excluded from the counting process.'
    else:
        text = ''

    data = data.format(basename, ext, text, mincount)
    with open(basename + '.txt', 'w') as fobj:
        fobj.write(data)

    with io.open(basename + ext, 'w', encoding='utf-8') as fobj:
        for docs in grouper(10000, it):
            rows = []
            for doc in docs:
                if doc is None:
                    break
                row = [doc['_id'], str(doc['count']), str(doc['user_count'])]
                rows.append(','.join(row))
            fobj.write('\n'.join(rows))

