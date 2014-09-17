"""
Plot user tweet activity.

"""
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import twitterproj

def plot_counts(collection, ax=None):
    if ax is None:
        ax = plt.gca()

    counts = []
    for doc in collection.find():
        counts.append(doc['count'])

    mean, std = np.mean(counts), np.std(counts)
    bincounted = np.bincount(counts)
    out = ax.plot(bincounted)
    return out, mean, std

def tweet_counts(hashtags=True, ax=None):
    db = twitterproj.connect()
    if hashtags:
        coll = db.userHashtagTweetCount
    else:
        coll = db.userTweetCount

    out, mean, std = plot_counts(coll, ax)
    ax = out[0].axes
    ax.set_xlabel('A user tweeted $x$ times')
    ax.set_ylabel('Number of users tweeting $x$ times')
    ax.set_yscale('log')
    ax.set_xscale('log')
    if hashtags:
        mod = 'Hashtag'
        ax.set_title('Hashtagged Tweets')
    else:
        mod = ''
        ax.set_title('All Tweets')
    ax.figure.savefig('user{0}TweetCounts.pdf'.format(mod))

def hashtag_counts(ax=None):
    db = twitterproj.connect()
    coll = db.hashtags
    out, mean, std = plot_counts(coll, ax)
    ax = out[0].axes
    ax.set_xlabel('A hashtag appears $x$ times')
    ax.set_ylabel('Number of hashtags appearing $x$ times')
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.figure.savefig('hashtagCounts.pdf')

def hashtag_botFiltered_counts(ax=None):
    db = twitterproj.connect()
    coll = db.hashtags.botFiltered
    out, mean, std = plot_counts(coll, ax)
    ax = out[0].axes
    ax.set_xlabel('A hashtag appears $x$ times')
    ax.set_ylabel('Number of hashtags appearing $x$ times')
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.figure.savefig('hashtagCountsBotFiltered.pdf')


if __name__ == '__main__':
    #f, ax = plt.subplots()
    #tweet_counts(hashtags=True, ax=ax)
    #f, ax = plt.subplots()
    #tweet_counts(hashtags=False, ax=ax)
    #f, ax = plt.subplots()
    #hashtag_counts(ax=ax)
    f, ax = plt.subplots()
    hashtag_botFiltered_counts(ax=ax)


