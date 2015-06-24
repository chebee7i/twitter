"""

Begin date: 2014-04-25 08:00 UTC
End date:   2015-04-25 08:00 UTC

Basic stats and counts for each hashtag are stored in an sqlite3 database:
hashtag_counts.db. The tweets were binned at a temporal resolution of 5
minutes. This means each hashtag has roughly 100k bins associated to it.
There is a single table "hashtags" which was constructed as:

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

Descriptions:
    hashtag
        The hashtag. Two special hashtags are included: "#world" and "#us".
        Note that this is safe since hashtags are not allowed to have "#" mark.
        So there is no conflict with the hashtags "world" and "us". Hashtags
        are stored in lowercase, but not necessarily in English.
    nnz_bins
        The number of 5-minute bins that have nonzero counts.
    range_bins
        The difference between the latest and earliest nonzero bins.
    min_bin
        The earliest nonzero bin.
    max_bin
        The latest nonzero bin.
    wmean_bin
        The weighted mean of nonzero bins: \sum_i w_i b_i / \sum_j w_j.
        This weights each bin by the number of counts in that bin.
    total_counts
        The total number of times the hashtag was mentioned in the data.
    mean_counts
        The mean count per bin.
    std_counts
        The standard deviation of counts per bin with ddof=0.
    mean_counts
        The mean count per nonzero bin.
    std_counts
        The standard deviation of counts per nonzero bin with ddof=0.
    counts
        A string representing a JSON object (similar to a Python dictionary)
        that maps bin indexes to count values. In Python, you'd run:
            import json
            c = json.loads(counts)

"""
from __future__ import division

import datetime
import json
import sqlite3

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
conn = sqlite3.connect(DB)

def fetch(hashtag, dense=True):
    """
    Returns the stored data for a given hashtag.

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

    return tuple(out)

def plot_usworld():
    """
    Plots counts as a function of days.

    """
    import matplotlib.pyplot as plt
    import seaborn

    us = fetch('#us', dense=True)[-1]
    world = fetch('#world', dense=True)[-1]

    days = DATES[::12 * 24]
    us_daily = [arr.sum() for arr in np.array_split(us, slots / (12 * 24))]
    world_daily = [arr.sum() for arr in np.array_split(world, slots / (12 * 24))]

    f, ax = plt.subplots()
    ax.plot(days[-200:], us_daily[-200:], label='US')
    ax.plot(days[-200:], world_daily[-200:], label='World')
    ax.set_title('Geotagged Hashtags')
    ax.set_ylabel('Count')
    ax.set_xlabel('Time')
    f.autofmt_xdate()
    plt.legend(loc='best')
    plt.savefig('hashtag_counts.pdf')

def fetch_counts(pre, repeater, post, repeats=50):
    counts = {}
    for k in range(1, repeats):
        hashtag = pre + k * repeater + post
        data = fetch(hashtag, dense=True)
        if data:
            print hashtag, data[6]
            counts[k] = data[6]
    return counts

def plot_repeaters(hashtags, kmax=20):
    """
    Plots the counts repeating symbols over different hashtags.

    hashtags should be a list of 3-tuples, each representing a family of
    hashtags. The 3-tuple should specify the pre, repeater, and post portion.

    """
    import matplotlib.pyplot as plt
    import seaborn
    plt.rcParams['text.usetex'] = False

    counts = []
    max_k = 0
    for hashtag in hashtags:
        c = fetch_counts(*hashtag)
        mx = max(list(c.keys()))
        if mx > max_k:
            max_k = mx
        counts.append(c)

    # Make counts dense
    max_k = min(kmax, max_k)
    kvals = range(1, max_k + 1)
    counts = [ [c.get(k, 0) for k in kvals] for c in counts ]

    f, ax = plt.subplots()
    for i, hashtag in enumerate(hashtags):
        if len(hashtag[1]) > 1:
            label = '${}({})^k{}$'.format(*hashtag)
        else:
            label = '${}{}^k{}$'.format(*hashtag)
        ax.plot(kvals, counts[i], marker='o', label=label, alpha=.5, clip_on=True)
        ax.set_title("Repeated symbols in hashtags")
    ax.set_yscale('log')
    ax.set_ylabel('Counts')
    ax.set_xlabel('k')
    plt.legend(loc='best')
    plt.savefig('hashtag_repeats.pdf')

def main():
    plot_usworld()
    """
    hashtags = [
        ('ye', 's', ''),
        ('n', 'o', ''),
        ('w', 'o', 'w'),
        ('wo', 'w', ''),
        ('', 'ha', ''),
        ('', 'jk', ''),
    ]
    plot_repeaters(hashtags)
    """

if __name__ == '__main__':
    main()
