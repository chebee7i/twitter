"""
Some really simple export scripts.

"""
from __future__ import unicode_literals

import io
import iterutils
import pymongo

import twitterproj

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return iterutils.izip_longest(fillvalue=fillvalue, *args)

def by_count(filename, collection):
    tweets, db, client = twitterproj.connect()
    chunksize = int(1e6)
    lines = []
    with io.open(filename, 'w', encoding='utf-8') as fobj:
        c = collection.find().sort('count', pymongo.DESCENDING)
        for chunk in grouper(chunksize, c, None):
            # Filler is None, so we make sure it is something.
            lines = ["{},{}".format(doc['_id'], int(doc['count'])) for doc in chunk if doc]
            fobj.write('\n'.join(lines))

def users_by_count(filename, hashtags=True):
    """
    Write users and the number of tweets to file.

    If hashtags is True, then only count the tweets that have hashtags.

    """
    tweets, db, client = twitterproj.connect()
    if hashtags:
        collection = db.userHashtagTweetCount
    else:
        collection = db.userTweetCount

    by_count(filename, collection)

def hashtags_by_count(filename):
    tweets, db, client = twitterproj.connect()
    by_count(filename, db.hashtags)

if __name__ == '__main__':
    tweets, db, client = twitterproj.connect()
    #hashtags_by_count('hashtags.csv')
    #users_by_count('userCounts_hashtag_tweets.csv', hashtags=True)
    #users_by_count('userCounts_all_tweets.csv', hashtags=False)
    by_count('hashtags_botFiltered.csv', db.hashtags.botFiltered)

