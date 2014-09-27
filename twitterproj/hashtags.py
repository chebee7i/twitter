from __future__ import unicode_literals

import collections
import io
import pymongo

from .helpers import connect

__all__ = ['sorted_hashtags', 'region_hashtags', 'region_hashtags_str']

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

def region_hashtags(region, hashtags, top_n):
    """
    Print the hashtag counts for a region.

    Hashtag counts are sorted according to global hashtag counts.
    Those hashtags in the top n of hashtag counts are indicated as such.

    """
    db = connect()
    top_ht = set(hashtags[:top_n])

    counts_region = region['counts']
    x = [(ht, counts_region[ht]) for ht in hashtags if ht in counts_region]

    counts = collections.OrderedDict()
    for ht, count in x:
        if ht in top_ht:
            counts[(ht, count)] = True
        else:
            counts[(ht, count)] = False

    return counts

def region_hashtags_str(region, hashtags, top_n):
    counts = region_hashtags(region, hashtags, top_n)
    lines = ["{},{},{}".format(htcount[0], htcount[1], top)
             for htcount, top in counts.items()]
    return '\n'.join(lines)
