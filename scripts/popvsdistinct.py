"""
Plot and calculate county population size to number of distinct hashtags.

"""
import matplotlib.pyplot as plt
import seaborn
import pandas
import twitterproj
import scipy.stats
import numpy as np

def populations():
    # Grab demographic info
    data = {}
    df = pandas.read_csv('../census/county/PEP_2013_PEPANNRES_with_ann.csv')
    for county in df.values[1:]: # Skip column headers
        fips = county[1]
        data[fips] = int(county[-1])
    return data

def distinct_ht():
    # Grab tweet info
    data = {}
    db = twitterproj.connect()
    for county in twitterproj.hashtag_counts__counties(db, bot_filtered=True):
        fips = county['geoid']
        #data[fips]['tweeted_hashtags'] = sum(county['counts'].values())
        data[fips] = len(county['counts'])
    return data

def main():
    pops = populations()
    dhts = distinct_ht()
    Y = np.array(dhts.values())
    X = np.array([pops[idx] for idx in dhts.keys()])
    r, p = scipy.stats.pearsonr(X, Y)
    print r , p
    plt.scatter(X, Y, s=1, marker='o')
    plt.savefig('popdistht.pdf')
    plt.title(r'$\rho = {}'.format(r))
if __name__ == '__main__':
    main()
