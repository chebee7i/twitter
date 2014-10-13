"""
Insert the number of users that tweeted each hashtag.

"""
import twitterproj
import pymongo
from collections import defaultdict
import json

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

def to_json(filename, bot_filtered=True):

    if bot_filtered:
        collection = db.hashtags.bot_filtered
    else:
        collection = db.hashtags

    rows = []
    for doc in collection.find():
        row = [doc['_id'], doc['count'], doc['user_count']]
        rows.append(row)

    data = {'data': rows}
    with open(filename, 'w') as fobj:
        json.dump(data, fobj)

