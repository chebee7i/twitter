"""
Write hashtag Frobenius scores to file.

"""
import io
from operator import itemgetter
import time

import numpy as np
from scipy.stats.mstats import mquantiles

import twitterproj
from twitterproj.fisher import *
from twitterproj.fisher import pipeline

db = twitterproj.connect()

def hashtag_scores():
    N = 5000

    lines = frobenius_hashtags(5000)
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


class Runner(object):
    def __init__(self, hashtags):
        self.hashtags = hashtags
    def __call__(self, k):
        htags = self.hashtags[:k]
        counties = frobenius_counties(htags)
        scores = [x[1] for x in counties]
        quants = mquantiles(scores)
        return quants

def county_scores(k=None, relative=True, to_csv=True):
    if k is None:
        import sys
        try:
            k = int(sys.argv[1])
        except IndexError:
            k = 50

    N = 5000
    lines = frobenius_hashtags(N)

    hashtags = [line[0] for line in lines]

    htags = hashtags[:k]
    counties = frobenius_counties(htags, relative=relative)

    import json
    d = {}
    for geoid, score, counts in counties:
        d[geoid] = score
    d['min'] = 0
    d['max'] = 1
    with open('json/grids.counties.bot_filtered.fisherscores.json', 'w') as f:
        json.dump(d, f)

    d = {}
    for geoid, score, counts in counties:
        d[geoid] = (score, counts)

    if to_csv:
        lines = []
        for geoid, score, counts in counties:
            line = [geoid, score]
            line.extend(counts)
            line = map(str, line)
            lines.append(','.join(line))
        header = '# geoid,{0}fisher score, [counts]'
        if relative:
            header = header.format(' relative ')
        else:
            header = header.format(' ')
        lines.insert(0, header)
        filename = 'hashtag_fisherscores_relative_n{0}.csv'
        filename = filename.format(k)
        with open(filename, 'w') as f:
            f.write('\n'.join(lines))

    return htags, counties, d

def county_quants(k=None):
    if k is None:
        import sys
        try:
            k = int(sys.argv[1])
        except IndexError:
            k = 50

    N = 5000
    lines = frobenius_hashtags(N)

    hashtags = [line[0] for line in lines]

    from multiprocessing import Pool
    import json
    p = Pool(22)
    kvals = range(10, 205, 5)
    runner = Runner(hashtags)
    quants = p.map(runner, kvals)
    quants = map(list, quants)

    d = [kvals, quants]
    with open('quants.json', 'w') as f:
        json.dump(d, f)

    return kvals, quants

def plot_quants(kvals, quants):
    import matplotlib.pyplot as plt
    import seaborn
    quants = np.array(quants)
    quants = quants.transpose()
    plt.plot(quants[0], 'o-', label="25th percentile")
    plt.plot(quants[1], 'o-', label="50th percentile")
    plt.plot(quants[2], 'o-', label="75th percentile")
    plt.ylabel('Relative Fisher Score')
    plt.xlabel('Number of Hashtags')
    plt.legend(loc='best')
    plt.savefig('fisherscores.pdf')

