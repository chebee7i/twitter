"""
Script to write a JSON file containing relevant meta information.

Each grid type and property is stored in a separate file.
Due to JSON not supporting NaN and Infinity, such values are
converted into strings and then stored in JSON. The receiving
code will need to handle those values appropriately.

The JSON file will be a dict mapping a unique identifier to the property.

    states
        id: fips, 2-character string

    counties
        id: geoid, 5-character string

    squares
        id: _id, integer specifying the i^th cell in the enumeration
        from the lower-left corner of the bounding box

Properties:

    demographics.json
        state fips
            name : state name
            abbr : state abbreviation
        county geoid
            name  : county name
            state : state fips

    grids.states.bot_filtered.{property}.json
    grids.counties.bot_filtered.{property}.json
        Keyed by state fips or county geoid.

        property                description
        --------------------------------------------------------------------
        respop72013             resident population 07/2013
        landarea                total land area
        tweeted_hashtags        the number of tweeted hashtags
        distinct_hashtags       the number of distinct hashtags
        users                   the number of users that tweeted in an area
        mle_entropy             entropy of hashtag distribution
        top5000ratios           missing ratios using top 5000 hashtags
        topX70ratios            included ratios for top X=70% hashtags

    grids.squares.bot_filtered.{property}.json
        Keyed by cell id as string

        property                description
        ------------------------------------------------------------------
        area                    total area (land+water)
        tweeted_hashtags        the number of tweeted hashtags
        distinct_hashtags       the number of distinct hashtags
        users                   the number of users that tweeted in an area
        mle_entropy             entropy of hashtag distribution
        top5000ratios           missing ratios using top 5000 hashtags
        topX70ratios            included ratios for top X=70% hashtags

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

def names():
    # We use a single file for this one.

    data = defaultdict(dict)

    # The abbreviations of the contiguous states
    desired = us.states.mapping('abbr', 'fips', us.STATES_CONTIGUOUS)

    # States
    with fiona.open('../tiger/tl_2014_us_state.shp') as f:
        for state in f:
            state = state['properties']
            fips = state['STATEFP']
            abbr = state['STUSPS']
            if abbr not in desired:
                continue
            data[fips]['name'] = state['NAME']
            data[fips]['abbr'] = abbr

    # Counties
    with fiona.open('../tiger/tl_2014_us_county.shp') as f:
        for county in f:
            county = county['properties']
            # Keep only counties in contiguous states.
            state_fips = county['STATEFP']
            if state_fips not in data:
                continue
            geoid = county['GEOID']
            data[geoid]['name'] = county['NAMELSAD']
            data[geoid]['state'] = state_fips

    with open('json/names.json', 'w') as f:
        json.dump(data, f)

    return data

def population():

    # State
    data_state = {}
    df = pandas.read_csv('../census/state/PEP_2013_PEPANNRES_with_ann.csv')
    for state in df.values[1:]: # Skip column headers
        fips = state[1]
        data_state[fips] = int(state[-1])

    vals = np.array(data_state.values())
    data_state['norm'] = vals.sum()
    data_state['min'] = vals.min()
    data_state['max'] = vals.max()

    with open('json/grids.states.bot_filtered.respop72013.json', 'w') as f:
        json.dump(data_state, f)

    # County
    data_county = {}
    df = pandas.read_csv('../census/county/PEP_2013_PEPANNRES_with_ann.csv')
    for county in df.values[1:]: # Skip column headers
        geoid = county[1]
        data_county[geoid] = int(county[-1])

    vals = np.array(data_county.values())
    data_county['norm'] = vals.sum()
    data_county['min'] = vals.min()
    data_county['max'] = vals.max()

    with open('json/grids.counties.bot_filtered.respop72013.json', 'w') as f:
        json.dump(data_county, f)

    return data_state, data_county

def landarea():

    # The abbreviations of the contiguous states
    desired = us.states.mapping('abbr', 'fips', us.STATES_CONTIGUOUS)

    # State
    data_state = {}
    with fiona.open('../tiger/tl_2014_us_state.shp') as f:
        for state in f:
            state = state['properties']
            fips = state['STATEFP']
            abbr = state['STUSPS']
            if abbr not in desired:
                continue
            data_state[fips] = state['ALAND']

    vals = np.array(data_state.values())
    data_state['norm'] = vals.sum()
    data_state['min'] = vals.min()
    data_state['max'] = vals.max()

    with open('json/grids.states.bot_filtered.landarea.json', 'w') as f:
        json.dump(data_state, f)

    # County
    data_county = {}
    with fiona.open('../tiger/tl_2014_us_county.shp') as f:
        for county in f:
            county = county['properties']
            # Keep only counties in contiguous states.
            state_fips = county['STATEFP']
            if state_fips not in data_state:
                continue
            geoid = county['GEOID']
            data_county[geoid] = county['ALAND']

    vals = np.array(data_county.values())
    data_county['norm'] = vals.sum()
    data_county['min'] = vals.min()
    data_county['max'] = vals.max()

    with open('json/grids.counties.bot_filtered.landarea.json', 'w') as f:
        json.dump(data_county, f)

    return data_state, data_county


def hashtags_and_entropy():

    db = twitterproj.connect()
    collkeys = [
        [twitterproj.hashtag_counts__states, 'fips'],
        [twitterproj.hashtag_counts__counties, 'geoid'],
        [twitterproj.hashtag_counts__squares, '_id'],
    ]
    # tweeted_hashtags, distint_hashtags, mle_entropy
    # for state, counties, squares
    dicts = [{}, {}, {}, {}, {}, {}, {}, {}, {}]

    for i, (coll, key) in enumerate(collkeys):
        print(coll)
        for region in coll(db, bot_filtered=True):
            region_id = region[key]
            dicts[3*i + 0][region_id] = sum(region['counts'].values())
            dicts[3*i + 1][region_id] = len(region['counts'])
            dicts[3*i + 2][region_id] = entropy(region)

    for i, data in enumerate(dicts):
        if (i+1) % 3 == 0:
            # This is an entropy dict
            # Need to remap "NaN" to np.nan
            vals = [v if v != 'NaN' else np.nan for v in data.values()]
        else:
            vals = data.values()
        vals = np.array(vals)
        data['norm'] = np.nansum(vals)
        data['min'] = np.nanmin(vals)
        data['max'] = np.nanmax(vals)

    filenames = [
        'json/grids.states.bot_filtered.tweeted_hashtags.json',
        'json/grids.states.bot_filtered.distinct_hashtags.json',
        'json/grids.states.bot_filtered.mle_entropy.json',
        'json/grids.counties.bot_filtered.tweeted_hashtags.json',
        'json/grids.counties.bot_filtered.distinct_hashtags.json',
        'json/grids.counties.bot_filtered.mle_entropy.json',
        'json/grids.squares.bot_filtered.tweeted_hashtags.json',
        'json/grids.squares.bot_filtered.distinct_hashtags.json',
        'json/grids.squares.bot_filtered.mle_entropy.json',
    ]

    for i, (fn, data) in enumerate(zip(filenames, dicts)):
        with open(fn, 'w') as f:
            json.dump(data, f)

def top5000ratios():

    filenames = [
        'top5000ratios_states.json',
        'top5000ratios_counties.json',
        'top5000ratios_squares.json',
    ]

    data = [{}, {}, {}]
    for i, filename in enumerate(filenames):
        with open(filename) as f:
            ratios = json.load(f)

        for key, ratio in ratios.items():
            # Python allows NaN storage in json.
            if np.isnan(ratio):
                # Convert to string NaN for the web
                ratio = "NaN"

            data[i][key] = ratio

        vals = ratios.values()
        data[i]['norm'] = np.nansum(vals)
        data[i]['min'] = np.nanmin(vals)
        data[i]['max'] = np.nanmax(vals)
        data[i]['min'] = 0
        data[i]['max'] = 1

    out_filenames = [
        'json/grids.states.bot_filtered.top5000ratios.json',
        'json/grids.counties.bot_filtered.top5000ratios.json',
        'json/grids.squares.bot_filtered.top5000ratios.json',
    ]
    for data, outfn in zip(data, out_filenames):
        with open(outfn, 'w') as f:
            json.dump(data, f)

def unionX():

    # Takes at least 1 hour.

    db = twitterproj.connect()
    collkeys = [
        [twitterproj.hashtag_counts__states, 'fips', 'states'],
        [twitterproj.hashtag_counts__counties, 'geoid', 'counties'],
        [twitterproj.hashtag_counts__squares, '_id', 'squares'],
    ]

    Xvals = [.7, .8, .9]
    for i, X in enumerate(Xvals):
        print i, X
        for region_iter, key, suffix in collkeys:
            filename = 'json/grids.{}.bot_filtered.unionX{}.json'
            filename = filename.format(suffix, int(X * 100))
            print filename

            hashtags = twitterproj.sorted_hashtags_unionX(X, region_iter())
            d = OrderedDict()
            norm = 0
            for region in region_iter():
                ratio, tagged = twitterproj.included_ratio(region['counts'],
                                                           hashtags)
                idx = region[key]
                if np.isnan(ratio):
                    ratio = "NaN"
                else:
                    norm += ratio
                d[idx] = ratio

                # Norm makes no sense for this...but whatever
                d['norm'] = norm
                d['min'] = 0
                d['max'] = 1

            with open(filename, 'w') as fobj:
                json.dump(d, fobj)


def make_all():
    names()
    population()
    landarea()
    hashtags_and_entropy()
    top5000ratios()
    unionX()

if __name__ == '__main__':
    #top5000ratios()
    unionX()
