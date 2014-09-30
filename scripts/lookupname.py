"""
Fetch user information by id.
"""

from __future__ import print_function

import sys
import tweepy
try:
    from tweepy.auth import OAuthHandler
except ImportError:
    # < tweepy 2.3
    from tweepy import OAuthHandler

import configparser

config = configparser.ConfigParser()
config.read('../project.cfg')
consumer_key = config['Twitter']['consumer_key']
consumer_secret = config['Twitter']['consumer_secret']
access_token = config['Twitter']['access_token']
access_token_secret = config['Twitter']['access_token_secret']

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

if __name__ == '__main__':
    try:
        user_ids = sys.argv[1].split(',')
    except IndexError:
        print("Usage: python lookupname.py 6253282,783214")
        sys.exit(1)
    else:
        out = api.lookup_users(user_ids=user_ids)
        for user in out:
            print("{}\t{}\t{}".format(user.id, user.screen_name, user.name))


