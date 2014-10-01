"""
Script used to store tweets using Twitter's streaming API.

A lockfile ensures that the script is not started again
if it is already running.

During streaming, a separate file `ratelimits.log` holds any received data
on ratelimits.

"""

from __future__ import print_function

import errno
import fcntl
import sys
import time

import logging
from logging.handlers import TimedRotatingFileHandler

import tweepy
from tweepy.auth import OAuthHandler
from tweepy.streaming import StreamListener, Stream

import json

import configparser

config = configparser.ConfigParser()
config.read('../project.cfg')
consumer_key = config['Twitter']['consumer_key']
consumer_secret = config['Twitter']['consumer_secret']
access_token = config['Twitter']['access_token']
access_token_secret = config['Twitter']['access_token_secret']

def is_ratelimit(tweet):
    """
    Returns True if the "tweet" is a rate-limit response.

    If False, then it should be an actual tweet.

    """
    if isinstance(tweet, dict):
        # The tweet is a Python dictionary
        if 'limit' in tweet and 'track' in tweet['limit']:
            return True
        else:
            return False
    else:
        # The tweet is a raw text string.
        if tweet.startswith('{"limit":{"track"'):
            return True
        else:
            return False

class StdOutListener(StreamListener):
    """
    A listener handles tweets that are the received from the stream.

    This is a basic listener that just prints received tweets to stdout.

    """
    def __init__(self, api=None, fobj=None):
        StreamListener.__init__(self, api)
        if fobj is None:
            fobj = sys.stdout
        self.fobj = fobj
        self.ratelimit = open('ratelimits.log', 'a')
        self.ratelimit.write('\n\n# Stream restarted on {0}'.format(time.strftime('%c')))

    def on_data(self, raw_data):
        # Note, the raw data is a utf-8 encoded json string.
        # We surpress the newline since the raw data already has one.
        print(raw_data, file=self.fobj, end='')

        if is_ratelimit(raw_data):
            data = '{0}\t{1}'.format(time.strftime("%c"), raw_data)
            print(data, file=self.ratelimit, end='')

    def on_error(self, status):
        print(status)

class RotatingLogListener(StreamListener):
    """
    A listener that writes UTF-8 encoded JSON tweets to rotating log files.

    Logs are rotated every 8 hours. Hardcoded. :)

    """
    def __init__(self, api=None, basename=None):
        StreamListener.__init__(self, api)
        if basename is None:
            basename = 'tweets'

        logHandler = TimedRotatingFileHandler("tweets", when="H", interval=8)
        logFormatter = logging.Formatter('%(message)s')
        logHandler.setFormatter(logFormatter)
        logger = logging.getLogger('MyLogger')
        logger.addHandler(logHandler)
        logger.setLevel(logging.INFO)

        self.logHandler = logHandler
        self.logger = logger
        self.ratelimit = open('ratelimit.log', 'a')
        data = "\n\n# Stream restarted: {0}\n".format(time.strftime('%c'))
        print(data, file=self.ratelimit)

    def on_data(self, raw_data):
        # Note, the raw data is a utf-8 encoded json string.
        # We strip off the trailing newline since the logger adds one for us.
        self.logger.info(raw_data[:-1])

        if is_ratelimit(raw_data):
            data = '{0}\t{1}'.format(time.strftime("%c"), raw_data)
            print(data, file=self.ratelimit, end='')

    def on_error(self, status):
        print(status)

def main_logger(basename=None):
        l = RotatingLogListener(basename=basename)
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = Stream(auth, l)

        # This fetches ANY geotagged tweet:
        # https://dev.twitter.com/docs/streaming-apis/parameters#locations
        stream.filter(locations=[-180,-90,180,90])

if __name__ == '__main__':
    # http://stackoverflow.com/questions/6146523/running-python-script-with-cron-only-if-not-running
    with open('.lock', 'w') as f:
        try:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError, e:
            if e.errno == errno.EAGAIN:
                msg = '[{0}] Script already running.\n'.format(time.strftime('%c'))
                sys.stderr.write(msg)
                sys.exit(-1)
            raise
        else:
            msg = '\n[{0}] Restarting script.\n'.format(time.strftime('%c'))
            sys.stderr.write(msg)
        main_logger()

