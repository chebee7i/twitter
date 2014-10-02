"""
Script to write a JSON file containing relevant meta information.

Meta information for state and counties grids will be combined.
Meta information for square grids will be in a separate file.

The JSON file will be a dict mapping a unique identifier to meta information
for the cell of the grid.

    states
        id:
            state fips, 2-str geoid from shapefile
        properties:
            2013 population est, land area, state name, state abbrev,
            tweeted_hashtags, distinct_hashtags, mle_entropy

    counties
        id:
            county fips, 5-str geoid from shapefile
        properties:
            2013 population est, land area, county name, state fips
            tweeted hashtags, distinct hashtags, mle_entropy

    square
        id:
            id from mongodb, specifying the ith cell in the enumeratation
            from the llc of the bounding box
        properties:
            number of tweets, area of cell (not land area)

"""
from collections import OrderedDict, defaultdict
import us
import fiona
import pandas
import dit
import twitterproj
import numpy as np
import json

def entropy(place):
    """
    Return the entropy of a county's hashtag counts.

    """
    pmf = np.array( place['counts'].values(), dtype=float)
    pmf /= pmf.sum()
    H = dit.shannon.entropy_pmf(pmf)
    return H

def state_info():
    data = defaultdict(dict)

    # Grab geographic info
    with fiona.open('../tiger/tl_2014_us_state.shp') as f:
        for state in f:
            state = state['properties']
            fips = state['STATEFP']
            data[fips]['name'] = state['NAME']
            data[fips]['landarea'] = state['ALAND']
            data[fips]['abbr'] = state['STUSPS']

    # Grab demographic info
    df = pandas.read_csv('../census/state/PEP_2013_PEPANNRES_with_ann.csv')
    for state in df.values[1:]: # Skip column headers
        fips = state[1]
        data[fips]['respop72013'] = int(state[-1])

    # Grab tweet info
    db = twitterproj.connect()
    for state in twitterproj.hashtag_counts__states(db, bot_filtered=True):
        fips = state['fips']
        data[fips]['tweeted_hashtags'] = sum(state['counts'].values())
        data[fips]['distinct_hashtags'] = len(state['counts'])
        data[fips]['mle_entropy'] = entropy(state)

    # Take only the contiguous states and DC
    states = OrderedDict()
    for state in us.STATES_CONTIGUOUS:
        fips = str(state.fips)
        states[fips] = data[fips]

    return states

def county_info():
    data = defaultdict(dict)

    # Grab geographic info
    with fiona.open('../tiger/tl_2014_us_county.shp') as f:
        for county in f:
            county = county['properties']
            fips = county['GEOID']
            data[fips]['name'] = county['NAMELSAD']
            data[fips]['landarea'] = county['ALAND']
            data[fips]['state'] = county['STATEFP']

    # Grab demographic info
    df = pandas.read_csv('../census/county/PEP_2013_PEPANNRES_with_ann.csv')
    for county in df.values[1:]: # Skip column headers
        fips = county[1]
        data[fips]['respop72013'] = int(county[-1])

    # Grab tweet info
    db = twitterproj.connect()
    for county in twitterproj.hashtag_counts__counties(db, bot_filtered=True):
        fips = county['geoid']
        data[fips]['tweeted_hashtags'] = sum(county['counts'].values())
        data[fips]['distinct_hashtags'] = len(county['counts'])
        data[fips]['mle_entropy'] = entropy(county)

    # Take only the counties in the contiguous states and DC.
    # The database only has these...so let's just loop through them again.
    counties = OrderedDict()
    for county in twitterproj.hashtag_counts__counties(db, bot_filtered=True):
        fips = county['geoid']
        counties[fips] = data[fips]

    return counties

def all_info():
    state = state_info()
    county = county_info()

    # Now also provide normalization info
    vals = np.array([ [p['respop72013'],
                       p['tweeted_hashtags'],
                       p['distinct_hashtags'],
                       p['landarea'],
                       p['mle_entropy']]
                       for p in state.values() ])
    norms = vals.sum(axis=0)
    mins = vals.min(axis=0)
    maxs = vals.max(axis=0)
    state['norms_state'] = {'respop72013': norms[0],
                            'tweeted_hashtags': norms[1],
                            'landarea': norms[2]};
    state['min_state'] = {'respop72013': mins[0],
                          'tweeted_hashtags': mins[1],
                          'distinct_hashtags': mins[2],
                          'landarea': mins[3],
                          'mle_entropy': mins[4]};
    state['max_state'] = {'respop72013': maxs[0],
                          'tweeted_hashtags': maxs[1],
                          'distinct_hashtags': maxs[2],
                          'landarea': maxs[3],
                          'mle_entropy': maxs[4]}

    vals = np.array([ [p['respop72013'],
                        p['tweeted_hashtags'],
                        p['distinct_hashtags'],
                        p['landarea'],
                        p['mle_entropy']]
                      for p in county.values() ])
    norms = vals.sum(axis=0)
    mins = vals.min(axis=0)
    maxs = vals.max(axis=0)
    county['norms_county'] = {'respop72013': norms[0],
                              'tweeted_hashtags': norms[1],
                              'landarea': norms[2]}
    county['min_county'] = {'respop72013': mins[0],
                            'tweeted_hashtags': mins[1],
                            'distinct_hashtags': mins[2],
                            'landarea': mins[3],
                            'mle_entropy': mins[4]};
    county['max_county'] = {'respop72013': maxs[0],
                            'tweeted_hashtags': maxs[1],
                            'distinct_hashtags': maxs[2],
                            'landarea': maxs[3],
                            'mle_entropy': maxs[4]};

    state.update(county)
    return state

if __name__ == '__main__':
    info = all_info()
    json.dump(info, open('state_county_info.json', 'w'))
