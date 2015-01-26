"""
Write hashtag Frobenius scores to file.

"""
import io
from operator import itemgetter
import time

import numpy as np

import twitterproj
from twitterproj.fisher import *
from twitterproj.fisher import pipeline

def main():
    N = 5000

    db = twitterproj.connect()
    collection = db.hashtags.bot_filtered

    hashtags = []
    hashtags.extend(top_hashtags(N, 'count', collection, extract=False))
    hashtags.extend(top_hashtags(N, 'user_count', collection, extract=False))

    hashtags_dict = {}
    for hashtag_doc in hashtags:
        ht = hashtag_doc['_id']
        hashtags_dict[ht] = hashtag_doc
    hashtags = list(hashtags_dict.keys())

    norms = [frobenius_norm]
    scores = []
    for norm in norms:
        start = time.time()
        scores.append(pipeline(N, hashtags, norm))
        stop = time.time()
        print norm, stop - start
    scores = list(zip(*scores))

    lines = []
    for i, hashtag in enumerate(hashtags):

        line = [
            hashtag,
            hashtags_dict[hashtag]['count'],
            hashtags_dict[hashtag]['user_count'],
            scores[i][0],
            #scores[i][1],
        ]
        lines.append(line)

    lines.sort(key=itemgetter(3))
    lines = [u','.join(map(unicode, line)) for line in lines]

    lines.insert(0, u'# hashtag, count, user count, frobenius norm of FIM')

    filename = 'htscores.csv'
    with io.open(filename, 'w') as fobj:
        fobj.write(u'\n'.join(lines))

def scatter():
    with open('htscores.csv') as f:
        lines = list(f.readlines())[1:]

    counts = []
    usercounts = []
    scores = []
    for line in lines:
        ht, c, uc, score = line.strip().split(',')
        counts.append(float(c))
        usercounts.append(float(uc))
        scores.append(float(score))

    import matplotlib.pyplot as plt
    plt.style.use('ggplot')

    f, axes = plt.subplots(1,2)
    axes = list(reversed(axes))

    plt.sca(axes[0])
    clip = None
    scat = axes[0].scatter(counts[:clip], usercounts[:clip], c=scores[:clip], s=10, cmap=plt.cm.cool, edgecolors='none', alpha=.2)
    cb = f.colorbar(scat)
    axes[0].set_xlabel('Hashtag Count')
    axes[0].set_ylabel('Hashtag User Count')
    axes[0].set_xscale('log')
    axes[0].set_yscale('log')
    cb.set_label('Frobenius Norm')
    if clip is not None:
        axes[0].set_title('Lowest {0} Scores'.format(clip))

    axes[1].hist(scores, bins=15)
    axes[1].set_xlabel('$d$, Frobenius Norm')
    axes[1].set_ylabel('Bin count of $d$')
    if clip is not None:
        axes[1].set_title('Histogram of all scores' )
    f.tight_layout()
    f.savefig('scores.pdf')

main()
