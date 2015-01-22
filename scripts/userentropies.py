"""
Plot the entropy of user usages by county.

"""

import sys
import twitterproj
import json
import matplotlib
import matplotlib.pyplot as plt

def collect_data():
    count = sys.argv[1]
    style = sys.argv[2]
    normalized = bool(int(sys.argv[3]))

    ents = twitterproj.userentropy__counties(count, style, normalized=normalized)
    fn = "userentropies_{0}_{1}_{2}.json"
    if normalized:
        fn = fn.format(count, style, 'norm')
    else:
        fn = fn.format(count, style, 'nonorm')

    with open(fn, 'w') as f:
        json.dump(ents, f)

def plot():
    numTweets_count = json.load(open('userentropies_numTweets_count_norm.json'))
    numTweetsWithHashtags_count = json.load(open('userentropies_numTweetsWithHashtags_count_norm.json'))

    numTweets_count_nonorm = json.load(open('userentropies_numTweets_count_nonorm.json'))
    numTweetsWithHashtags_count_nonorm = json.load(open('userentropies_numTweetsWithHashtags_count_nonorm.json'))

    population = json.load(open('json/grids.counties.bot_filtered.respop72013.json'))

    counties = list(numTweets_count.keys())

    w = [numTweets_count[geoid] for geoid in counties]
    x = [numTweetsWithHashtags_count[geoid] for geoid in counties]
    y = [numTweets_count_nonorm[geoid] for geoid in counties]
    z = [numTweetsWithHashtags_count_nonorm[geoid] for geoid in counties]
    c = [population[geoid] for geoid in counties]

    #matplotlib.style.use('fivethirtyeight')
    import seaborn

    f, (ax1, ax2) = plt.subplots(1,2)
    ax1.scatter(w, x, c=c, s=5, alpha=.5, edgecolors='none',
                norm=matplotlib.colors.LogNorm(), cmap=matplotlib.cm.GnBu)
    cax = ax2.scatter(y, z, c=c, s=5, alpha=.5, edgecolors='none',
                      norm=matplotlib.colors.LogNorm(), cmap=matplotlib.cm.GnBu)

    ax1.set_xlabel('Normalized User Entropy of Tweets')
    ax1.set_ylabel('Normalized User Entropy of Hashtagged Tweets')
    ax1.set_xlim(0.0001, 0.15)
    ax1.set_ylim(0.0001, 0.15)
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    ax2.set_xlabel('User Entropy of Tweets')
    ax2.set_ylabel('User Entropy of Hashtagged Tweets')

    cb = f.colorbar(cax)
    cb.set_label("Log Population")

    f.savefig('UserEntropyByCounty.pdf')

plot()
#collect_data()
