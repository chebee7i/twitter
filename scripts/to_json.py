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
    # Handle NaN since json can't.
    if np.isnan(H):
        H = "NaN"
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
        # Handle nan since json can't.
        H = entropy(state)
        data[fips]['mle_entropy'] = entropy(state)

    # Missing ratios
    ratios = json.load(open('top5000ratios_states.json'))
    for fips, ratio in ratios.items():
        if np.isnan(ratio):
            ratio = "NaN"
        data[fips]['top5000ratios'] = ratio

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

    # Missing ratios
    ratios = json.load(open('top5000ratios_counties.json'))
    for fips, ratio in ratios.items():
        data[fips]['top5000ratios'] = ratio

    # Take only the counties in the contiguous states and DC.
    # The database only has these...so let's just loop through them again.
    counties = OrderedDict()
    for county in twitterproj.hashtag_counts__counties(db, bot_filtered=True):
        fips = county['geoid']
        counties[fips] = data[fips]

    return counties

def square_info():
    data = defaultdict(dict)

    # Grab tweet info
    db = twitterproj.connect()
    for square in db.grids.squares.bot_filtered.find():
        idx = square['_id']
        data[idx]['tweeted_hashtags'] = sum(square['counts'].values())
        data[idx]['distinct_hashtags'] = len(square['counts'])
        data[idx]['mle_entropy'] = entropy(square)

    # Missing ratios
    ratios = json.load(open('top5000ratios_squares.json'))
    for idx, ratio in ratios.items():
        if np.isnan(ratio):
            ratio = "NaN"
        idx = int(idx)
        data[idx]['top5000ratios'] = ratio

    return data

def state_final():
    state = state_info()

    # Now also provide normalization info
    vals = np.array([ [p['respop72013'],
                       p['tweeted_hashtags'],
                       p['distinct_hashtags'],
                       p['landarea'],
                       p['mle_entropy']
                        if p['mle_entropy']!='NaN' else np.nan,
                       p['top5000ratios']
                        if p['top5000ratios']!='NaN' else np.nan]
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
                          'mle_entropy': mins[4],
                          'top5000ratios': mins[5]};
    state['max_state'] = {'respop72013': maxs[0],
                          'tweeted_hashtags': maxs[1],
                          'distinct_hashtags': maxs[2],
                          'landarea': maxs[3],
                          'mle_entropy': maxs[4],
                          'top5000ratios': maxs[5]}
    return state

def county_final():
    county = county_info()
    # County
    vals = np.array([ [p['respop72013'],
                        p['tweeted_hashtags'],
                        p['distinct_hashtags'],
                        p['landarea'],
                        p['mle_entropy'],
                        p['top5000ratios'],]
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
                            'mle_entropy': mins[4],
                            'top5000ratios': mins[5]};
    county['max_county'] = {'respop72013': maxs[0],
                            'tweeted_hashtags': maxs[1],
                            'distinct_hashtags': maxs[2],
                            'landarea': maxs[3],
                            'mle_entropy': maxs[4],
                            'top5000ratios': maxs[5]};
    return county

def square_final():
    square = square_info()

    vals = np.array([ [ p['tweeted_hashtags'],
                        p['distinct_hashtags'],
                        #p['landarea'],
                       p['mle_entropy']
                        if p['mle_entropy']!='NaN' else np.nan,
                       p['top5000ratios']
                        if p['top5000ratios']!='NaN' else np.nan,
                      ]
                      for p in square.values() ])
    norms = np.nansum(vals, axis=0)
    mins = np.nanmin(vals, axis=0)
    maxs = np.nanmax(vals, axis=0)
    square['norms_square'] = {'tweeted_hashtags': norms[0],
                              #'landarea': norms[2]
                             }
    square['min_square'] = {'tweeted_hashtags': mins[0],
                            'distinct_hashtags': mins[1],
                            #'landarea': mins[3],
                            'mle_entropy': mins[2],
                            'top5000ratios': mins[3]};
    square['max_square'] = {'tweeted_hashtags': maxs[0],
                            'distinct_hashtags': maxs[1],
                            #'landarea': maxs[3],
                            'mle_entropy': maxs[2],
                            'top5000ratios': maxs[3]};
    return square

def state_and_county():
    state = state_final()
    county = county_final()
    state.update(county)
    return state

if __name__ == '__main__':
    #info = state_and_county()
    #json.dump(info, open('state_county_info.json', 'w'))

    info = square_final()
    json.dump(info, open('grids.squares.info.json', 'w'))
