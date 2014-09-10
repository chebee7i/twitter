"""
Helper functions for the raw parsing of tweets.

Most everything in here, except for Tweet() and connect()
will only be used during the parsing and building of the database.

Note: geoJSON formatted coordinates are: (longitude, latitude)

"""
from __future__ import unicode_literals
from __future__ import print_function

import glob
import io
import json
import os
import shutil
import subprocess
import sys

from collections import OrderedDict
from datetime import datetime

from shapely.geometry import box, asShape

# Bounding boxes are two geoJSON points specifying the
# (A) lower left and (B) upper right corners of a box.
#
# bbox = [left, bottom, right, top]
#      = [min Longitude , min Latitude , max Longitude , max Latitude]
#      = [ A.longitude, A.latitude, B.longitude, B.latitude]
#
ANY = box(-180, -90, 180, 90)
USA = box(-124.7625, 24.5210, -66.9326, 49.3845) # continental

def create_index(filename, force=False):
    """
    Create an index from a text file of tweets.

    Assumption: Each line corresponds to a tweet.

    The index can be used to randomly access each tweet. It will contain seek
    locations to each tweet. For example, the 3rd line of the index file will
    contain the seek location of the 3rd tweet in the original file.

    Parameters
    ----------
    filename : str
        The name of the tweet file.
    force : bool
        If true, then create the index file even if an index file
        already exists.

    """
    if os.path.isfile(filename + '.index') and not force:
        return

    lines = []
    # Open in binary mode, since we want raw byte locations.
    with open(filename, 'rb') as fobj:
        lines.append(0)
        # Do not use the file object as an iterator.
        # http://stackoverflow.com/questions/14145082/file-tell-inconsistency
        # Instead, use readline.
        for line in iter(fobj.readline, ''):
            lines.append(fobj.tell())
    with open(filename + '.index', 'w') as fobj:
        fobj.write('\n'.join(map(str, lines)))

def is_ratelimit(tweet):
    """
    Returns True if the "tweet" is a rate-limit response.

    If False, then it should be an actual tweet.

    Works with raw tweet (as str) or de-JSONified tweet (as dict).

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

def location_match(tweet, polygon, allow_place=True):
    """
    Returns `True` if the tweet's location is in the bounding box.

    We follow Twitter's rules to determine location.
    See: https://dev.twitter.com/docs/streaming-apis/parameters#locations

    1) If the coordinates field is populated, the values there will be tested
    against the bounding box. Note that this field uses geoJSON order
    (longitude, latitude).

    2) If coordinates is empty but place is populated, the region defined in place
    is checked for intersection against the locations bounding box. Any overlap
    will match.

    3) If none of the rules listed above match, the Tweet does not match the
    location query. Note that the geo field is deprecated, and ignored by the
    streaming API.

    Parameters
    ----------
    tweet : dict
        The Python representation of the tweet.
    box : Shapely Polygon
        Usually, this will be a rectangular bounding box.

    Returns
    -------
    match : bool
        True if the tweet's location matches the polygon.

    """
    match = False

    if 'coordinates' in tweet and tweet['coordinates']:
        # Method 1) Check location for an intersection.
        shape = asShape(tweet['coordinates'])
        match = polygon.intersects(shape)
    elif allow_place and 'place' in tweet and tweet['place']:
        # Method 2) Check place for an intersection.
        shape = asShape(tweet['place']['bounding_box'])
        match = polygon.intersects(shape)
    else:
        match = False

    return match

def tweets(filename, with_ratelimits=False):
    """
    Simple parsing of tweets.

    If with_ratelimits is `True`, then ratelimit JSON objects are returned
    in addition to tweet JSON objects.

    """
    with open(filename, 'r') as fobj:
        location = 0
        for i, line in enumerate(iter(fobj.readline, b'')):
            line = line.strip()
            if not line:
                continue
            try:
                tweet = json.loads(line)
            except ValueError:
                # For some reason, some lines written to file included
                # bunch of null characters \x00. We need to remove them
                # before we get a proper JSON decoding.
                try:
                    line2 = line.replace('\x00', '')
                    tweet = json.loads(line2)
                except ValueError:
                    msg = "JSON error decoding line {0} of {1} at {2} bytes:\n{3!r}"
                    msg = msg.format(i, filename, location, line)
                    raise ValueError(msg)
            else:
                location = fobj.tell()
            if with_ratelimits:
                yield tweet
            elif not is_ratelimit(tweet):
                yield tweet

def us_geocoded_tweets(filename):
    """
    Iterator over US geocoded tweets.

    """
    for tweet in tweets(filename, with_ratelimits=False):
        if location_match(tweet, USA, allow_place=False):
            yield tweet

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

def get_hashtags(tweet):
    hashtags = []
    if 'entities' in tweet and 'hashtags' in tweet['entities']:
        for hashtag_dict in tweet['entities']['hashtags']:
            # Received hashtags are case-sensitive and are also displayed
            # case-sensitively in their tweets. However, Twitter treats
            # hashtags case-insensitively for tracking purposes.
            # So we lowercase them all in the database since we want to minimize
            # tracking errors and have no plans to do a study on how users tend
            # to visually display their hashtags.
            hashtags.append(hashtag_dict['text'].lower())
    return hashtags

def extract(tweet):
    """
    Builds an ordered dictionary of tweet data to be retained/stored.

    """
    template = '%a %b %d %H:%M:%S +0000 %Y'

    tweet_dt = datetime.strptime(tweet['created_at'], template)
    profile_dt = datetime.strptime(tweet['user']['created_at'], template)

    user = [
        ('id', tweet['user']['id']),
        ('favourites_count', tweet['user']['favourites_count']),
        ('followers_count', tweet['user']['followers_count']),
        ('friends_count', tweet['user']['friends_count']),
        ('statuses_count', tweet['user']['statuses_count']),
        ('created_at', profile_dt)
    ]
    user = OrderedDict(user)

    data = [
        ('created_at', tweet_dt),
        ('coordinates', tweet['coordinates']['coordinates']),
        ('hashtags', get_hashtags(tweet)),
        ('text', tweet['text']),
        ('lang', tweet['lang']),
        ('user', user)
    ]
    data = Tweet(data)
    return data

def counts(filename):
    """
    Return counts and categories for tweets from a raw text file.

    """
    total = 0
    ratelimits = 0
    geocoded = 0
    usa_geo= 0
    usa_nogeo = 0
    for tweet in tweets(filename, with_ratelimits=True):
        total += 1
        if is_ratelimit(tweet):
            ratelimits += 1
            continue

        match = location_match(tweet, USA)
        if 'coordinates' in tweet and tweet['coordinates']:
            geocoded += 1
            if match:
                usa_geo += 1
        elif match:
            usa_nogeo += 1

    print("""Total: {0}
Rate limits: {1}
Geocoded: {2}
USA: {3}
USA w/geo: {4}
""".format(total, ratelimits, geocoded, usa_geo + usa_nogeo, usa_geo))

def test_run(filename, count=10):
    """
    Prints the first 10 US geocoded tweets from `filename`.

    """
    for i, tweet in enumerate(us_geocoded_tweets(filename)):
        if i > count:
            break
        try:
            t = extract(tweet)
        except:
            #print(repr(tweet))
            raise
        else:
            print(t, end='\n\n')
        i += 1

def connect():
    """
    Connect to a local mongo database for tweets.

    """
    from pymongo import MongoClient
    client = MongoClient()
    db = client.twitter
    collection = db.tweets

    return collection, db, client

def take(n, iterable):
    """Return first n items of the iterable as a list.

    """
    return list(islice(iterable, n))

def insert_chunked(filename, db, chunksize=10**5, log=True, dry_run=False):
    """
    Insert tweets in mongodb database.

    """
    import os
    sources = set([source['filename']
        for source in db.sources.find(fields={'filename':1})])
    basename = os.path.basename(filename)
    if basename in sources:
        print("Already inserted.")
        return
    collection = db.tweets
    tweets = []
    count = 0
    print("Chunk: {0}".format(count))
    for i, tweet in enumerate(us_geocoded_tweets(filename)):
        if log and i % (chunksize/10) == 0:
            print("\t{0}".format(i))
        t = extract(tweet)
        tweets.append(t)
        if len(tweets) == chunksize:
            if not dry_run:
                collection.insert(tweets)
            tweets = []
            count += 1
            print("Chunk: {0}".format(count))

    else:
        if not dry_run:
            collection.insert(tweets)

    # Mark this filename as done.
    if not dry_run:
        db.sources.insert({'filename':basename})


class Pipeline(object):
    def __init__(self, db, dry_run=False):
        self.db = db
        self.dry_run = dry_run
    def __call__(self, gzfilename):
        shutil.copy(gzfilename, '/tmp')
        tmpfilename = '/tmp/{0}'.format(os.path.basename(gzfilename))
        subprocess.call(['gunzip', tmpfilename])
        unzipped = tmpfilename[:-3]
        insert_chunked(unzipped, self.db, dry_run=self.dry_run)
        os.unlink(unzipped)

def populate_db(path, dry_run=False):
    """
    Populating the database from gzipped files found in `path`.

    """
    if path.endswith('/'):
        path = path[:-1]
    filenames = glob.glob(path + '/*.gz')

    collection, db, client = connect()
    p = Pipeline(db, dry_run=dry_run)

    sources = set([source['filename']
        for source in db.sources.find(fields={'filename':1})])

    for filename in filenames:
        print("Inserting from {0}".format(filename))
        basename = os.path.basename(filename)[:-3]
        if basename in sources:
            print("Already inserted.")
            continue
        elif '-09-' in basename:
            print("Skipping tweets past the stop date.")
            continue
        else:
            sys.stdout.flush()
            p(filename)

