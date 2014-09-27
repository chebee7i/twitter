import collections
import io
import pymongo

from .helpers import connect

__all__ = ['top_hashtags']

def top_hashtags(n, bot_filtered=True, filename=None):
    """
    Returns the `n` most frequently used hashtags.

    """
    db = connect()
    if bot_filtered:
        collection = db.hashtags.bot_filtered
    else:
        collection = db.hashtags

    c = collection.find().sort('count', pymongo.DESCENDING).limit(n)
    hashtags = list(c)

    if filename is not None:
        with io.open(filename, 'w', encoding='utf-8') as f:
            f.write("# hashtag,hashtag count\n")
            lines = []
            for hashtag in hashtags:
                line = "{},{}".format(hashtag['_id'], int(hashtag['count']))
                lines.append(line)
            f.write('\n'.join(lines))

    return [hashtag['_id'] for hashtag in hashtags]


