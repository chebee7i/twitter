"""

"""
from __future__ import unicode_literals
from __future__ import print_function

import itertools
import os
import sys
from collections import OrderedDict
from datetime import datetime
from multiprocessing import Pool

from shapely.geometry import box, asShape

import pymongo

class Tweet(OrderedDict):

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        """
        A simple print of the tweet.

        """
        from StringIO import StringIO
        f = StringIO()
        pad = 13
        pad_inner = 18
        template = " {0:<11} : {1}"
        for i, (key, val) in enumerate(self.items()):
            if key != 'user':
                print(template.format(key, val), file=f)
            else:
                # 'user' is the last one
                print(template.format(key, ''), file=f)
                template2 = "    {0:<18} : {1}"
                for j, (ikey, ival) in enumerate(val.items()):
                    print(template2.format(ikey, ival), file=f)
        f.seek(0)
        return f.read()[:-1] # remove the last newline

    def location(self):
        """
        Returns a shapely object representing the location of the tweet.

        """
        # Construct a geoJSON input for shapely.
        coords = {'coordinates': self['coordinates'], 'type': 'Point'}
        shape = asShape(coords)
        return shape

def connect():
    """
    Connect to the local mongo database for tweets.

    """
    from pymongo import MongoClient
    client = MongoClient()
    db = client.twitter
    tweets = db.tweets

    return tweets, db, client

