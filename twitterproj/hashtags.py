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
    'region_hashtags',
    'region_hashtags_str',
    'region_missing_tophashtags',
    'missing_ratio',
]

def sorted_hashtags(bot_filtered=True, filename=None):
    """
    Returns the `n` most frequently used hashtags.

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

def region_hashtags(region, hashtags, top_n=None):
    """
    Print the hashtag counts for a region.

    Hashtag counts are sorted according to global hashtag counts.
    Those hashtags in the top n of hashtag counts are indicated as such.

    The region should have bot_filtered counts if the hashtags passed in
    are bot_filtered hashtags.

    """
    db = connect()
    if top_n is not None:
        top_ht = set(hashtags[:top_n])
    else:
        top_ht = set(hashtags)

    counts_region = region['counts']
    x = [(ht, counts_region[ht]) for ht in hashtags if ht in counts_region]

    counts = collections.OrderedDict()
    for ht, count in x:
        if ht in top_ht:
            counts[(ht, count)] = True
        else:
            counts[(ht, count)] = False

    return counts

def region_missing_tophashtags(region, hashtags, local_top_n=None, global_top_n=None):
    """
    Returns the top n hashtags wrt to the local region
    that are not in the top n hashtags wrt to the total dataset.

    If local_top_n is None, then it returns the hashtags in the local region
    which are not in the top n hashtags of the total dataset.

    If global_top_n is None, then all hashtags are considered and this
    function is basically useless.

    On its own, this is a deceptive measure, since many regions might not even
    have as many distinct hashtags as the value of local_top_n. A more
    important quantity is the ratio of the total number of tweeted hashtags
    from the missing tophashtags relative to the total tweeted hashtags
    in the region amongst its regional top n. See missing_ratio()


    """
    counts = region_hashtags(region, hashtags, top_n=global_top_n)
    ig0 = itemgetter(0)
    ig1 = itemgetter(1)
    keyfunc = lambda x: ig1(ig0(x))
    local_sort = counts.items()
    local_sort.sort(key=keyfunc, reverse=True)
    top_local = local_sort[:local_top_n]
    # The ones with the value False are not in the global_top_n.
    top_local_missing = [ht for ht, in_global in top_local if not in_global]
    top_local_dict = collections.OrderedDict([ht for ht, in_global in top_local])
    return collections.OrderedDict(top_local_missing), top_local_dict

def missing_ratio(top_local_missing, top_local):
    """
    A value of 1 means that none of local top hashtags are in the
    global top hashtags.

    A value of 0 means that all of the local top hashtags are present in the
    global top hashtags.

    """
    z = np.asarray(top_local.values(), dtype=float).sum()
    return np.asarray(top_local_missing.values(), dtype=float).sum() / z


def region_hashtags_str(region, hashtags, top_n):
    counts = region_hashtags(region, hashtags, top_n)
    lines = ["{},{},{}".format(htcount[0], htcount[1], top)
             for htcount, top in counts.items()]
    return '\n'.join(lines)
