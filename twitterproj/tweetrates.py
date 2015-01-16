"""
Calculate tweet rates for each country or user.

"""
from __future__ import division

from collections import defaultdict
import datetime

import twitterproj

def tweet_rates__counties(db=None):
    """
    Calculates tweet rates for each county.

    """
    if db is None:
        db = twitterproj.connect()

    collection = db.grids.userstats.counties.bot_filtered

    dates = defaultdict(lambda: [datetime.datetime(2020,1,1), datetime.datetime(1900,1,1)])
    tweets = defaultdict(int)

    for countyuser in collection.find():
        user_id = countyuser['user_id']
        geoid = countyuser['geoid']

        mn_dt = countyuser['created_at'][0]
        if mn_dt < dates[geoid][0]:
            dates[geoid][0] = mn_dt

        mx_dt = countyuser['created_at'][-1]
        if mx_dt > dates[geoid][1]:
            dates[geoid][1] = mx_dt

        mn_count = countyuser['statuses_count'][0]
        mx_count = countyuser['statuses_count'][-1]
        count = mx_count - mn_count
        if count == 0:
            count = 1
        tweets[geoid] += count

    rates = {}
    for geoid in dates:
        days = (dates[geoid][1] - dates[geoid][0]).days
        if days == 0:
            days = 1
        count = tweets[geoid]
        rate = count / days
        rates[geoid] = rate

    return rates

def user_rate(doc):
    dt_mn = doc['created_at'][0]
    dt_mx = doc['created_at'][-1]
    days = (dt_mx - dt_mn).days
    if days == 0:
        days = 1
    count = doc['statuses_count'][-1] - doc['statuses_count'][0]
    if count == 0:
        count = 1
    return count / days

def tweet_rates__counties_meanuser(db=None):
    """
    Calculates mean tweet rates over users for each county.

    """
    if db is None:
        db = twitterproj.connect()

    collection = db.grids.userstats.counties.bot_filtered

    rates = defaultdict(lambda : [0, 0])
    for countyuser in collection.find():
        user_id = countyuser['user_id']
        geoid = countyuser['geoid']
        key = (geoid, user_id)

        rate = user_rate(countyuser)

        # Cumulative moving average
        oldcount, oldrate = rates[geoid]
        newcount = oldcount + 1
        newrate = (rate + oldcount * oldrate) / newcount
        rates[geoid] = [newcount, newrate]

    for geoid in rates:
        rates[geoid] = rates[geoid][1]

    return rates


def tweet_rates__users(db=None):
    """
    Calculates tweet rates for each user.

    """
    if db is None:
        db = twitterproj.connect()

    collection = db.grids.userstats.counties.bot_filtered

    dates = defaultdict(list)
    tweets = defaultdict(list)

    for countyuser in collection.find():
        user_id = countyuser['user_id']
        dates[user_id].append(countyuser['created_at'][0])
        dates[user_id].append(countyuser['created_at'][-1])
        tweets[user_id].append(countyuser['statuses_count'][0])
        tweets[user_id].append(countyuser['statuses_count'][-1])

    rates = {}
    for user_id in dates:
        dt = sorted(dates[user_id])
        days = (dt[-1] - dt[0]).days
        if days == 0:
            days = 1
        min_count = min(tweets[user_id])
        max_count = max(tweets[user_id])
        count = max_count - min_count
        if count == 0:
            count = 1
        rate = count / days
        rates[user_id] = rate

    return rates
