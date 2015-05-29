"""
Once counter.py has created all the json files, we need to collate the data.

"""
from __future__ import division

import os
import glob
import sqlite3
from collections import defaultdict
import json
import numpy as np
import time
import ago
import datetime
import multiprocessing
import itertools
import tempfile

def grouper(iterable, n, fillvalue=None):
    """
    grouper('ABCDEFG', 3, 'x') --> 'ABC' 'DEF' 'Gxx'

    """
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)

def create_tables(filename):
    conn = sqlite3.connect(filename)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS hashtags (hashtag text unique, nnz int, total int,
        mean real, stddev real, nonzero_mean real, nonzero_std real, counts text);
    """)
    return conn

sources = glob.glob("tweets*.json")
conn = create_tables('hashtag_counts.db')

binsize = 60 / 5
length = binsize * 24 * 365

def zero_vec():
    return np.zeros(length, dtype=int)

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            o2 = o.tolist()
        except TypeError:
            pass
        else:
            return o2

        return json.JSONEncoder.default(self, o)

def worker(sources):

    world = np.zeros(length, dtype=int)
    us = np.zeros(length, dtype=int)
    us_check = np.zeros(length, dtype=int)
    hashtags = defaultdict(zero_vec)

    for i, source in enumerate(sources):
        if source is None:
            # Using grouper() can cause some sources to be None.
            continue

        print(source)
        with open(source) as fobj:
            data = json.load(fobj)
            world += data['all_tweetcounts']
            us += data['us_tweetcounts']

            L = len(data['us_htcounts'])
            percentJump = 0.1
            step = int(L * percentJump)

            print("Number of hashtags: {}".format(L))
            it = enumerate(data['us_htcounts'].iteritems())
            for j, (hashtag, sp_counts) in it:

                div, mod = divmod(j, step)
                if mod == 0:
                    msg = r"{0}% complete for {1}.".format(div * percentJump * 100, source)
                    print(msg)

                indexes = np.array([int(k) for k in sp_counts.keys()])
                counts = np.array([int(c) for c in sp_counts.values()])
                hashtags[hashtag][indexes] += counts
                #us_check[indexes] += counts

    # Now add the us and world to hashtags.
    # No hashtag can begin with a "#", so this will clearly separate the world and us counts.
    hashtags['#world'] += world
    hashtags['#us'] += us


    print('writing to file')
    dirname = os.path.dirname(__file__)
    f = tempfile.NamedTemporaryFile(dir=dirname, prefix='stage1_', suffix='.json', delete=False)
    print(f.name)
    json.dump(hashtags, f, cls=JSONEncoder)
    print("done")

    return None

def combine(hashtags_dicts):

    hashtags = defaultdict(zero_vec)
    for hashtag_dict in hashtag_dicts:
        for hashtag, counts in hashtag_dict.iteritems():
            hashtags[hashtag] += counts

    return hashtags

def merge_counts(source, target):
    # Does not assume target is a defaultdict
    for index, count in source.iteritems():
        target[index] = target.get(index, 0) + count

#sources = sources[:3]
def cumulative_counts():
    hashtags = set([])
    cumcounts = []
    for source in sources:
        print source
        with open(source) as fobj:
            data = json.load(fobj)
            ht = data['us_htcounts']
            hashtags.update(ht.keys())
            cumcounts.append(len(hashtags))
            print cumcounts[-50:]
    print(len(hashtags))




def runner():
    nChunks = 10
    chunksize = int(np.ceil(len(sources) / nChunks))
    for group in grouper(sources, nChunks):
        worker2(group)

def worker2(sources):

    world = np.zeros(length, dtype=int)
    us = np.zeros(length, dtype=int)

    select = """SELECT counts FROM hashtags WHERE hashtag = ?"""
    update = """INSERT OR REPLACE INTO hashtags(hashtag, counts) VALUES (?, ?)"""

    for i, source in enumerate(sources):
        if source is None:
            # Using grouper() can cause some sources to be None.
            continue

        print(source)
        with open(source) as fobj:
            data = json.load(fobj)
            world += data['all_tweetcounts']
            us += data['us_tweetcounts']

            L = len(data['us_htcounts'])
            percentJump = 0.1
            step = int(L * percentJump)
            print("Number of hashtags: {}".format(L))

            hashtags = defaultdict(lambda: defaultdict(int))
            timediffs = []
            start = time.time()
            it = enumerate(data['us_htcounts'].iteritems())

            def flush():
                for hashtag, counts in hashtags.items():
                    with conn:
                        stored_counts = conn.execute(select, (hashtag,)).fetchone()
                        if stored_counts is not None:
                            stored_counts = json.loads(stored_counts[0])
                        else:
                            stored_counts = {}
                        for index, count in counts.iteritems():
                            stored_counts[index] = stored_counts.get(index, 0) + count
                        data = (hashtag, json.dumps(stored_counts))
                        conn.execute(update, data)

                hashtags.clear()

            for j, (hashtag, htcounts) in it:

                percent, remainder = divmod(j, step)
                if remainder == 0:
                    curr = time.time()
                    if j >= 3:
                        timediffs.append(curr - start)
                    start = curr
                    msg = r"{0}% complete.".format(percent / percentJump)

                    if j >= 3:
                        # The mean number of seconds per percent
                        avgtime = np.mean(timediffs)
                        timeleft = (1 - percent) * avgtime
                        delta = datetime.timedelta(seconds=timeleft)
                        future = datetime.datetime.now() + delta
                        msg2 = " Estimated completion {0}".format(ago.human(future))
                        msg += msg2
                    print(msg)

                    # Flush buffer
                    #flush()

                for index, count in htcounts.iteritems():
                    hashtags[hashtag][index] += count

            else:
                # Flush buffer
                flush()

    # Now add the us and world to hashtags.
    # No hashtag can begin with a "#", so this will clearly separate the world and us counts.
    with conn:
        for ht, data in [("#world", world), ("#us", us)]:
            stored = conn.execute(select, (ht,)).fetchone()
            if stored is not None:
                stored = np.asarray(json.loads(stored[0]))
            else:
                stored = zero_vec()

            stored += data
            data = (ht, json.dumps(stored.tolist()))
            conn.execute(update, data)

def postfix():
    import json
    hashtags = ['#world', '#us']

    update = """INSERT OR REPLACE INTO hashtags(hashtag, counts) VALUES (?, ?)"""

    for hashtag in hashtags:
        x = fetch(hashtag)
        dense = np.array(json.loads(x[-1]))
        indexes = np.nonzero(dense)[0]
        counts = dense[indexes]
        d = dict(zip(indexes, counts))
        dstr = json.dumps(d)
        with conn:
            conn.execute(update, (hashtag, dstr))


def fetch(hashtag):
    select = """SELECT * FROM hashtags WHERE hashtag = ?"""
    with conn:
        stored = conn.execute(select, (hashtag,)).fetchone()

    return stored
