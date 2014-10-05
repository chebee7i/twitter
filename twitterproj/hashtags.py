from __future__ import unicode_literals
from __future__ import division

import collections
import io
import pymongo
from operator import itemgetter

import numpy as np

from .helpers import connect

__all__ = [
    'sorted_hashtags',
    'sorted_hashtags_unionX',
    'region_sorted_hashtags',
    'keep_X_hashtags',
    'included_ratio',
]

def sorted_hashtags(bot_filtered=True, filename=None):
    """
    Returns all hashtags sorted such that the most frequently appears first.

    """
    db = connect()
    if bot_filtered:
        collection = db.hashtags.bot_filtered
    else:
        collection = db.hashtags

    hashtags = list(collection.find().sort('count', pymongo.DESCENDING))

    if filename is not None:
        with io.open(filename, 'w', encoding='utf-8') as f:
            f.write("# hashtag,hashtag count\n")
            lines = []
            for hashtag in hashtags:
                line = "{},{}".format(hashtag['_id'], int(hashtag['count']))
                lines.append(line)
            f.write('\n'.join(lines))

    return [hashtag['_id'] for hashtag in hashtags]

def sorted_hashtags_unionX(X, region_iter, filename=None):
    """
    Returns the union of hashtags over a grid, using they hashtags in each
    region that comprise at least X*100 percent of the hashtag counts within
    the region. The union is then sorted according to the global count.

    If this hashtag list is used to build distributions, then covereage in
    each region of the grid will be at least X*100 percent, but probably higher
    due to the union.

    """
    union = collections.defaultdict(int)
    for region in region_iter:
       sorted_counts = region_sorted_hashtags(region)
       keep = keep_X_hashtags(sorted_counts, X)
       for ht in keep:
           union[ht] += sorted_counts[ht]

    out = union.items()
    out.sort(key=key)
    out = collections.OrderedDict(out)

    if filename is not None:
        with io.open(filename, 'w', encoding='utf-8') as f:
            f.write("# hashtag,hashtag count\n")
            lines = []
            for hashtag, count in out.items():
                line = "{},{}".format(hashtag, count)
                lines.append(line)
            f.write('\n'.join(lines))

    return out

def key(x):
    # Descending by count, then lexico by hashtag.
    return (-x[1], x[0])

def region_sorted_hashtags(region):
    """
    Returns hashtag counts for a region, sorted within the region.

    """
    counts = region['counts'].items()
    counts.sort(key=key)
    return collections.OrderedDict(counts)

def keep_X_hashtags(counts, X):
    """
    Returns the hashtags from `counts` whose summed count is ratio X of
    the total hashtag counts within the region.

    Parameters
    ----------
    counts : OrderedDict
        The regionally-sorted hashtag counts.
    X : float
        A value in [0,1] that represents the minimum target ratio
        of hashtag count mass.

    """
    vals = np.array(counts.values(), dtype=float)
    vals /= vals.sum()
    sums = vals.cumsum()
    i = 0
    for i, s in enumerate(sums):
        if s >= X:
            break
    else:
        i += 1
    return counts.keys()[:i]

def included_ratio(counts, hashtags, topN=None):
    """
    Returns the ratio of hashtag counts to total hashtag counts for those
    hashtags appearing in `hashtags`. Also returns a dictionary tagging
    each hashtag as included or not.

    Optionally, calculate the ratio using only the top N local hashtags.

    The output is a dictionary mapping hashtags to (count, included) tuples,
    where `count` is the number of times the hashtag was mentioned in a
    region, and `included` is a boolean that is true if the hastag was in
    `hashtags`.

    """
    if not isinstance(hashtags, set):
        hashtags = set(hashtags)

    counts = counts.items()
    if topN is not None:
        counts = counts[:topN]

    nIncluded = 0
    nTotal = 0
    d = collections.OrderedDict()
    for hashtag, count in counts:
        included = hashtag in hashtags
        d[hashtag] = (count, included)
        if included:
            nIncluded += count
        nTotal += count

    # Possibly returns NaN
    ratio = np.array(float(nIncluded)) / np.array(float(nTotal))

    return ratio, d

