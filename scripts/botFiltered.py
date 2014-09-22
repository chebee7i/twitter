"""
The goal is to build a bot-filtered list of hashtags.

We implement:

        > db.runCommand({
         aggregate: "tweets.with_hashtags",
         pipeline: [{$match: {"user.id" : {$nin : [LIST OF IDS]}}},
                    {$unwind: "$hashtags"},
                    {$group: {_id:"$hashtags", count: {$sum:1}}},
                    {$out: "hashtags.bot_filtered"}],
         allowDiskUse: true})

"""
import pymongo
import json
import bson

import twitterproj

BOTFILE = 'data/botlist_first_1000.csv'
HASHTAGFILE = 'data/bad_hashtags_botFiltered.csv'

def get_bots(botfile=None):
    """
    Returns the user ids of the identified bots.

    """
    if botfile is None:
        botfile = BOTFILE

    with open(botfile, 'rU') as fobj:
        ids = fobj.readlines()

    # Remove newlines
    ids = map(str.strip, ids)

    # Convert to int (not really necesary)
    ids = map(int, ids)

    return ids

def get_hashtags(hashtagfile=None):
    """
    Returns a list of hashtags.

    Assumes CSV: with trailing comma...just because that is what we have.
    Assumes lowercase hashtags.

    """
    if hashtagfile is None:
        hashtagfile = HASHTAGFILE

    with open(hashtagfile, 'rU') as fobj:
        hashtags = fobj.readlines()

    # Remove newlines and comma
    hashtags = [h[:-2] for h in hashtags]
    return hashtags

def insert_flagged_users(uids, **kwargs):
    """
    Flags users by id.

    Example
    ------
    >>> uids = get_bots('/path/to/uids')
    >>> insert_flagged_users(uids, first_1000=True, avoid=True)

    """
    db = twitterproj.connect()
    col = db.users.flagged
    for uid in uids:
        col.update({'_id': uid}, {"$set" : kwargs}, upsert=True)

def insert_flagged_hashtags(hashtags, **kwargs):
    """
    Flags hashtag by id.

    Example
    -------
    >>> hashtags = get_hashtags('/path/to/hashtags')
    >>> insert_flagged_hashtags(hashtags, autobot=True, avoid=True)

    """
    db = twitterproj.connect()
    col = db.hashtags.flagged
    for hashtag in hashtags:
        col.update({'_id': hashtag}, {"$set": kwargs}, upsert=True)

def users_using_flagged_hashtags(criteria):
    """
    Returns user ids for uses who tweeted hashtags to be avoided.

    Examples
    --------
    >>> users_using_flagged_hashtags({'avoid': True})
    >>> users_using_flagged_hashtags({'autobot': True})

    """
    db = twitterproj.connect()
    col = db.hashtags.flagged
    hashtags = list(col.find(criteria, {'_id': True}))
    hashtags = [ht['_id'] for ht in hashtags]
    hashtags = set(hashtags)

    users = set([])
    for tweet in db.tweets.with_hashtags.find():
        if hashtags.intersection(set(tweet['hashtags'])):
            users.add(tweet['user']['id'])

    return users

def autoflag_users():
    """
    Autoflag users who sent out a tweet with an "autobot" flagged hashtag.

    This takes about 20 minutes.

    """
    db = twitterproj.connect()
    users = users_using_flagged_hashtags({'autobot': True})
    insert_flagged_users(users, by_hashtag=True)

    # Now go through and make sure each user has all three properties.
    for user in db.users.flagged.find():
        if 'by_hashtag' not in user:
            user['by_hashtag'] = False
        if 'first_1000' not in user:
            user['first_1000'] = False
        if 'avoid' not in user:
            user['avoid'] = False
        db.users.flagged.update({'_id': user['_id']}, {"$set": user})


def export_autoflagged_users(filename, user_collection=None):
    """
    Writes autoflagged users to file, sorted by the number of tweets.

    """
    db = twitterproj.connect()
    if user_collection is None:
        user_collection = db.users

    flagged = list(db.users.flagged.find())
    uids = [f['_id'] for f in flagged]
    counts = list(user_collection.find({"_id": {"$in": uids}}))
    counts = dict([(c['_id'], int(c['count'])) for c in counts])

    tuples = [(counts[f['_id']],
               int(f['first_1000']),
               int(f['by_hashtag']),
               int(f['avoid']),
               f['_id']) for f in flagged]

    tuples.sort(reverse=True)
    lines = ['{}    {}  {}  {}  {}'.format(str(t[0]).rjust(8), t[1], t[2], t[3],
                                           t[4]) for t in tuples]


    with open(filename, 'w') as fobj:
        fobj.write("# Columns are: tweet count, first_1000, by_hashtag, avoid, id\n")
        fobj.write('\n'.join(lines))

    return counts


def build_command(botfile=None):
    ids = json.dumps(get_bots(botfile))
    template = """db.runCommand({{
         aggregate: "tweets.with_hashtags",
         pipeline: [{{$match: {{"user.id" : {{$nin : {ids}}}}}}},
                    {{$unwind: "$hashtags"}},
                    {{$group: {{_id:"$hashtags", count: {{$sum:1}}}}}},
                    {{$out: "hashtags.bot_filtered"}}],
         allowDiskUse: true}})"""
    return template.format(ids=ids)

def build_collection(botfile=None):
    """
    Builds the bot-filtered collection.

    """
    ids = get_bots(botfile)
    db = twitterproj.connect()
    db.tweets.with_hashtags.aggregate(
        [
            {"$match": {"user.id" : {"$nin" : ids}}},
            {"$unwind": "$hashtags"},
            {"$group": {"_id": "$hashtags", "count": {"$sum": 1}}},
            {"$out": "hashtags.bot_filtered"}
        ],
        allowDiskUse=True
    )
    db.hashtags.botFiltered.create_index([("count", pymongo.DESCENDING)])


