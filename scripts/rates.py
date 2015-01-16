"""
Plot tweet rates.

"""
import operator
from collections import OrderedDict
import twitterproj
import json

import matplotlib.pyplot as plt
import numpy as np
import seaborn

def user_rates(ax=None):
    if ax is None:
        ax = plt.gca()
    rates = twitterproj.tweet_rates__users()
    import json
    with open('savedrates.json', 'w') as f:
        json.dump(rates, f)

    mn = np.min(rates.values())
    mx = np.max(rates.values())
    mn = np.floor(np.log(mn))
    mx = np.ceil(np.log(mx))
    bins = np.logspace(mn, mx, 50)
    ax.hist(rates.values(), bins=bins)
    ax.set_xlabel(r'$r_u =~\text{Tweets per day}$')
    ax.set_ylabel('$p(r_u)$')
    ax.set_xscale('log')
    ax.set_title('Distribution of user tweet rates')

    plt.savefig('user_rates.pdf')
    ax.set_yscale('log')
    plt.savefig('user_rates_log.pdf')
    return ax

def county_meanuser(ax=None):
    with open('json/grids.counties.bot_filtered.respop72013.json') as f:
        populations = json.load(f)

    rates = twitterproj.tweet_rates__counties_meanuser()

    y = [rates[geoid] for geoid in rates]
    x = [populations[geoid] for geoid in rates]

    if ax is None:
        ax = plt.gca()

    ax.scatter(x, y, alpha=.5, edgecolors='none')
    mean = np.mean(y)
    ax.axhline(mean, color='k')
    ax.set_ylabel(r'$\langle r_u \rangle =~\text{Mean user-tweets per day}$')
    ax.set_xlabel('2013 Population Estimate')
    ax.set_xscale('log')
    ax.set_title('Mean user tweet rates per county')

    plt.savefig('county_meanuser.pdf')
    ax.set_yscale('log')
    plt.savefig('county_meanuser_log.pdf')

    return ax

def county(ax=None):
    with open('json/grids.counties.bot_filtered.respop72013.json') as f:
        populations = json.load(f)

    rates = twitterproj.tweet_rates__counties()

    y = [rates[geoid] for geoid in rates]
    x = [populations[geoid] for geoid in rates]

    if ax is None:
        ax = plt.gca()

    ax.scatter(x, y, alpha=.5, edgecolors='none')
    mean = np.mean(y)
    ax.axhline(mean, color='k')
    ax.set_ylabel(r'$\langle r_u \rangle =~\text{Tweets per day}$')
    ax.set_xlabel('2013 Population Estimate')
    ax.set_xscale('log')
    ax.set_title('Tweet rates per county')

    plt.savefig('county.pdf')
    ax.set_yscale('log')
    plt.savefig('county_log.pdf')
    return ax, rates
