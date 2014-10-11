"""
To each region, let's add the number of users that tweeted in the region.

"""
import json
import pymongo
import twitterproj

def via_json():

    raise Exception("""This script doesn't work for states because CA has two
documents. Fix this!""")


    json_files = [
        'json/grids.states.bot_filtered.users.json',
        'json/grids.counties.bot_filtered.users.json',
        'json/grids.squares.bot_filtered.users.json'
    ]

    db = twitterproj.connect()
    collections = [
        db.grids.states.bot_filtered,
        db.grids.counties.bot_filtered,
        db.grids.squares.bot_filtered,
    ]

    keys = [
        'fips',
        'geoid',
        '_id',
    ]

    for fn, coll, key in zip(json_files, collections, keys):
        print fn
        avoid = {'min', 'max', 'norm'}
        with open(fn) as f:
            userCounts = json.load(f)

        uc = {}
        for k, v in userCounts.items():
            if k in avoid:
                continue
            elif key == '_id':
                # The keys need to be int.
                k = int(k)
            uc[k] = v
        userCounts = uc

        for k, count in userCounts.items():
            if k in avoid:
                continue
            coll.update({key:k}, {"$set": {"user_count": count}}, upsert=False)

        coll.create_index([("user_count", pymongo.DESCENDING)])

