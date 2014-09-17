"""
The goal is to build a bot-filtered list of hashtags.

We implement:

        > db.runCommand({
         aggregate: "hashtagTweets",
         pipeline: [{$match: {"user.id" : {$nin : [LIST OF IDS]}}},
                    {$unwind: "$hashtags"},
                    {$group: {_id:"$hashtags", count: {$sum:1}}},
                    {$out: "hashtags.botFiltered"}],
         allowDiskUse: true})

"""
import pymongo
import json

import twitterproj

BOTFILE = 'botlist_first_1000.csv'

def get_bots(botfile=None):
    """
    Returns the user ids of the identified bots.

    """
    if botfile is None:
        botfile = BOTFILE

    with open(botfile) as fobj:
        ids = fobj.readlines()

    # Remove newlines
    ids = map(str.strip, ids)

    # Convert to int (not really necesary)
    ids = map(int, ids)

    return ids

def build_command(botfile=None):
    ids = json.dumps(get_bots(botfile))
    template = """db.runCommand({{
         aggregate: "hashtagTweets",
         pipeline: [{{$match: {{"user.id" : {{$nin : {ids}}}}}}},
                    {{$unwind: "$hashtags"}},
                    {{$group: {{_id:"$hashtags", count: {{$sum:1}}}}}},
                    {{$out: "hashtags.botFiltered"}}],
         allowDiskUse: true}})"""
    return template.format(ids=ids)

def build_collection(botfile=None):
    """
    Builds the bot-filtered collection.

    """
    ids = get_bots(botfile)
    db = twitterproj.connect()
    db.hashtagTweets.aggregate(
        [
            {"$match": {"user.id" : {"$nin" : ids}}},
            {"$unwind": "$hashtags"},
            {"$group": {"_id": "$hashtags", "count": {"$sum": 1}}},
            {"$out": "hashtags.botFiltered"}
        ],
        allowDiskUse=True
    )
    db.hashtags.botFiltered.create_index([("count", pymongo.DESCENDING)])

if __name__ == '__main__':
    build_collection()

