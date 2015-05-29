"""
Script used to populate statistic info about hashtags.

Begin date: 2014-04-25 08:00 UTC
End date:   2015-04-25 08:00 UTC

"""
from __future__ import division

import datetime
import json
import sqlite3
import time
import ago

from pytz import utc
import numpy as np

START_DATE = utc.localize(datetime.datetime(2014, 04, 25, 8))
STOP_DATE = utc.localize(datetime.datetime(2015, 04, 25, 8))

n_days = (STOP_DATE - START_DATE).days
resolution = 5 # in minutes
slots = int(n_days * 24 * 60 / resolution)

DELTA = datetime.timedelta(minutes=resolution)
DATES = [START_DATE + i * DELTA for i in range(slots)]

DB = 'hashtag_counts.db'
DB2 = 'hashtag_counts_new.db'
conn = sqlite3.connect(DB)

def fetch(hashtag, dense=True):
    """
    Returns the stored data for a given hashtag.

    The data is:

        hashtag, nnz, total, mean, stddev, nonzero_mean, nonzero_stddev, counts

    If dense is True, the counts are returned as a NumPy array instead of
    a dictionary.

    """
    select = """SELECT * FROM hashtags WHERE hashtag = ?"""
    with conn:
        stored = conn.execute(select, (hashtag,)).fetchone()

    if stored is None:
        return None

    out = list(stored)
    counts = out[-1]
    d = json.loads(counts)

    indexes = map(int, d.keys())
    counts = d.values()
    d = dict(zip(indexes, counts))

    if dense:
        x = np.array([ d.get(idx, 0) for idx in range(slots) ])
        out[-1] = x

    return tuple(out), stored

def plot_usworld():
    import matplotlib.pyplot as plt
    import seaborn

    us = fetch('#us', dense=True)[-1]
    world = fetch('#world', dense=True)[-1]

    days = DATES[::12 * 24]
    us_daily = [arr.sum() for arr in np.array_split(us, slots / (12 * 24))]
    world_daily = [arr.sum() for arr in np.array_split(world, slots / (12 * 24))]

    f, ax = plt.subplots()
    ax.plot(days, us_daily, label='US')
    ax.plot(days, world_daily, label='World')
    ax.set_title('All Hashtags')
    ax.set_ylabel('Count')
    ax.set_xlabel('Time')
    f.autofmt_xdate()
    plt.savefig('hashtag_counts.pdf')

def stats(hashtag, row=False):
    x, raw = fetch(hashtag, dense=True)
    counts = x[-1]
    nonzero = np.nonzero(counts)[0]

    nnz_bins = len(nonzero)
    range_bins = nonzero[-1] - nonzero[0]
    min_bin = nonzero[0]
    max_bin = nonzero[-1]
    wmean_bin = (counts[nonzero] * nonzero).sum() / counts[nonzero].sum()
    total_counts = counts.sum()
    mean_counts = np.mean(counts)
    std_counts = np.std(counts, ddof=0)
    nonzero_mean_counts = np.mean(counts[nonzero])
    nonzero_std_counts = np.std(counts[nonzero], ddof=0)

    data = (nnz_bins, range_bins, min_bin, max_bin, wmean_bin, total_counts, mean_counts, std_counts, nonzero_mean_counts, nonzero_std_counts)

    if row:
        data = (hashtag,) + data + (raw[-1],)

    return data

def create_tables(filename):
    conn = sqlite3.connect(filename)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS hashtags (
        hashtag text unique,
        nnz_bins int,
        range_bins int,
        min_bin int,
        max_bin int,
        wmean_bin real,
        total_counts int,
        mean_counts real,
        std_counts real,
        nonzero_mean_counts real,
        nonzero_std_counts real,
        counts text
    );
    """)
    return conn

insert = """INSERT INTO hashtags VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

def calculate_stats():

    import sys
    i = int(sys.argv[1])

    hashtags = conn.execute('SELECT hashtag from hashtags')
    hashtags = [row[0] for row in hashtags]

    nproc = 15
    hashtags = np.array_split(hashtags, nproc)[i]
    worker(hashtags, i)

    """
    import multiprocessing
    pool = multiprocessing.Pool(processes=nproc)
    for i in range(nproc):
        pool.apply_async(worker, args=(splits[i], i) )
    pool.close()
    pool.join()
    """

def worker(hashtags, taskid):
    conn_new = create_tables(DB2)
    L = len(hashtags)
    percentJump = 0.01
    step = int(L * percentJump)
    print "Step size:", step
    timediffs = []
    start = time.time()
    rows = []
    for i, hashtag in enumerate(hashtags):
        rows.append(stats(hashtag, row=True))

        percent, remainder = divmod(i, step)

        if remainder == 0:
            with conn_new:
                conn_new.executemany(insert, rows)
            rows = []

            curr = time.time()
            if i >= 3:
                interval = curr - start
                timediffs.append(interval)
                interval_delta = datetime.timedelta(seconds=interval)
                print "{0}:\tInterval: {1}".format(taskid, ago.human(interval_delta))
            start = curr
            msg = "{0}:\t{1}% complete.".format(taskid, percent * percentJump * 100)

            if i >= 3:
                # The mean number of seconds per percent
                avgtime = np.mean(timediffs)
                timeleft = (1 - percent * percentJump) / percentJump * avgtime
                delta = datetime.timedelta(seconds=timeleft)
                future = datetime.datetime.now() + delta
                msg2 = " Estimated completion {0}".format(ago.human(future))
                msg += msg2
            print(msg)
    else:
        with conn_new:
            conn_new.executemany(insert, rows)

    print "{0}:\tFinished.".format(taskid)

calculate_stats()
