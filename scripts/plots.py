"""
Plot user tweet activity.

"""
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import twitterproj

def plot_counts(collection, ax=None, key='count'):
    if ax is None:
        ax = plt.gca()

    counts = []
    for doc in collection.find():
        counts.append(doc[key])

    mean, std = np.mean(counts), np.std(counts)
    bincounted = np.bincount(counts)
    out = ax.plot(bincounted)
    return out, mean, std

def tweet_counts(hashtags=True, ax=None):
    db = twitterproj.connect()
    if hashtags:
        coll = db.users.with_hashtags
    else:
        coll = db.users

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
    coll = db.hashtags.bot_filtered
    out, mean, std = plot_counts(coll, ax)
    ax = out[0].axes
    ax.set_xlabel('A hashtag appears $x$ times')
    ax.set_ylabel('Number of hashtags appearing $x$ times')
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.figure.savefig('hashtagCountsBotFiltered.pdf')

def hashtag_usercounts(ax=None):
    db = twitterproj.connect()
    coll = db.hashtags
    out, mean, std = plot_counts(coll, ax=ax, key='user_count')
    ax = out[0].axes
    ax.set_xlabel('A hashtag was tweeted by $x$ users')
    ax.set_ylabel('Number of hashtags tweeted by $x$ users')
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_title('Hashtag User Counts')
    ax.figure.savefig('hashtagUserCounts.pdf')

def hashtag_botFiltered_usercounts(ax=None):
    db = twitterproj.connect()
    coll = db.hashtags.bot_filtered
    out, mean, std = plot_counts(coll, ax=ax, key='user_count')
    ax = out[0].axes
    ax.set_xlabel('A hashtag was tweeted by $x$ users')
    ax.set_ylabel('Number of hashtags tweeted by $x$ users')
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_title('Hashtag (Bot-filtered) User Counts')
    ax.figure.savefig('hashtagUserCountsBotFiltered.pdf')

def hashtag_usercount_limit(ax=None):
    db = twitterproj.connect()
    coll = db.hashtags.bot_filtered
    yvals = []
    xvals = []
    x = 1
    while True:
        c = coll.find({'user_count':{"$gte":x}}).count()
        yvals.append(c)
        xvals.append(x)
        if c == 0:
            break
        else:
            x += 1

    ax.plot(xvals, yvals)
    ax.set_xlabel('x, minimum number of user counts per hashtag')
    ax.set_ylabel('number of hashtags with at least x user counts')
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.figure.savefig('hashtagsUserCountBotFilteredLimit.pdf')

if __name__ == '__main__':
    #f, ax = plt.subplots()
    #tweet_counts(hashtags=True, ax=ax)
    #f, ax = plt.subplots()
    #tweet_counts(hashtags=False, ax=ax)
    #f, ax = plt.subplots()
    #hashtag_usercounts(ax=ax)
    f, ax = plt.subplots()
    #hashtag_botFiltered_usercounts(ax=ax)
    hashtag_usercount_limit(ax=ax)


