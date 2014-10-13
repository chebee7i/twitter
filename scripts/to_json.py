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

    #import sys
    #args = map(int, sys.argv[1:3])
    #collkeys = [ collkeys[args[0]] ]
    #Xvals = [ Xvals[args[1]] ]

    for i, X in enumerate(Xvals):
        print i, X
        for region_iter, key, suffix in collkeys:
            filename = 'json/grids.{}.bot_filtered.unionX{}.json'
            filename = filename.format(suffix, int(X * 100))
            print filename

            hashtags = twitterproj.sorted_hashtags_unionX(X, region_iter())
            d = OrderedDict()
            norm = 0
            ratio_min = np.inf
            ratio_max = -np.inf
            for region in region_iter():
                ratio, tagged = twitterproj.included_ratio(region['counts'],
                                                           hashtags)
                idx = region[key]
                if np.isnan(ratio):
                    ratio = "NaN"
                else:
                    if ratio < ratio_min:
                        ratio_min = ratio
                    if ratio > ratio_max:
                        ratio_max = ratio
                    norm += ratio
                d[idx] = ratio

                # Norm makes no sense for this...but whatever
                d['norm'] = norm
                d['min'] = ratio_min
                d['max'] = ratio_max

            with open(filename, 'w') as fobj:
                json.dump(d, fobj)

def unionXY():

    # Takes at least 1 hour.

    db = twitterproj.connect()
    collkeys = [
        [twitterproj.hashtag_counts__states, 'fips', 'states'],
        [twitterproj.hashtag_counts__counties, 'geoid', 'counties'],
        [twitterproj.hashtag_counts__squares, '_id', 'squares'],
    ]

    Xvals = [.7, .8, .9]
    Yvals = [ 100, 1000, 5000]
    #import sys
    #args = map(int, sys.argv[1:3])
    #collkeys = [ collkeys[args[0]] ]
    #Xvals = [ Xvals[args[1]] ]

    for i, X in enumerate(Xvals):
        for j, Y in enumerate(Yvals):
            print i, X
            for region_iter, key, suffix in collkeys:
                filename = 'json/grids.{}.bot_filtered.unionX{}_Y{}.json'
                filename = filename.format(suffix, int(X * 100), Y)
                print filename

                hashtags = twitterproj.sorted_hashtags_unionXY(X, Y, region_iter())
                d = OrderedDict()
                norm = 0
                ratio_min = np.inf
                ratio_max = -np.inf
                for region in region_iter():
                    ratio, tagged = twitterproj.included_ratio(region['counts'],
                                                               hashtags)
                    idx = region[key]
                    if np.isnan(ratio):
                        ratio = "NaN"
                    else:
                        if ratio < ratio_min:
                            ratio_min = ratio
                        if ratio > ratio_max:
                            ratio_max = ratio
                        norm += ratio
                    d[idx] = ratio

                    # Norm makes no sense for this...but whatever
                    d['norm'] = norm
                    d['min'] = ratio_min
                    d['max'] = ratio_max

                with open(filename, 'w') as fobj:
                    json.dump(d, fobj)


def users():
    db = twitterproj.connect()

    filenames = [
        'json/grids.states.bot_filtered.users.json',
        'json/grids.counties.bot_filtered.users.json',
        'json/grids.squares.bot_filtered.users.json',
    ]

    skip_users = set(twitterproj.subcollections.get_skip_users())
    counts = [OrderedDict(), OrderedDict(), OrderedDict()]

    def user_states():
        # States
        # The abbreviations of the contiguous states
        desired = us.states.mapping('abbr', 'fips', us.STATES_CONTIGUOUS)

        with fiona.open('../tiger/tl_2014_us_state.shp') as f:
            for state in f:
                users = set([])
                fips = state['properties']['STATEFP']
                abbr = state['properties']['STUSPS']
                if abbr not in desired:
                    continue
                for tweet in twitterproj.tweets_in_region(db.tweets.with_hashtags,
                                                          state['geometry']):
                    userid = tweet['user']['id']
                    if userid not in skip_users:
                        users.add(userid)
                else:
                    counts[0][fips] = len(users)
                    print(fips, counts[0][fips])

    def user_counties():
        # Counties
        geoids = db.grids.counties.bot_filtered.find({}, {'geoid': True})
        geoids = [g['geoid'] for g in geoids]
        geoids = set(geoids)
        with fiona.open('../tiger/tl_2014_us_county.shp') as f:
            for region in f:
                users = set([])
                key = region['properties']['GEOID']
                if key not in geoids:
                    continue
                for tweet in twitterproj.tweets_in_region(db.tweets.with_hashtags,
                                                          region['geometry']):
                    userid = tweet['user']['id']
                    if userid not in skip_users:
                        users.add(userid)
                else:
                    counts[1][key] = len(users)
                    print(key, counts[1][key])

    def user_squares():
        # Squares
        for i, region in enumerate(twitterproj.hashtag_counts__squares()):
            users = set([])
            key = region['_id']
            for tweet in twitterproj.tweets_in_region(db.tweets.with_hashtags,
                                                      region['geometry']):
                userid = tweet['user']['id']
                if userid not in skip_users:
                    users.add(userid)
            else:
                counts[2][key] = len(users)
                print(key, counts[2][key])

    hack = None
    """
    import sys
    hack = int(sys.argv[1])
    if hack == 0:
        print "States"
        user_states()
    elif hack == 1:
        print "Counties"
        user_counties()
    else:
        print "Squares"
        user_squares()

    if hack is not None:
        filenames = [filenames[hack]]
        counts = [counts[hack]]

    """

    for i, data in enumerate(counts):
        vals = data.values()
        vals = np.array(vals)
        data['norm'] = np.nansum(vals)
        data['min'] = np.nanmin(vals)
        data['max'] = np.nanmax(vals)

    for i, (fn, data) in enumerate(zip(filenames, counts)):
        with open(fn, 'w') as f:
            json.dump(data, f)

def count_ratio():
    """
    Probabilities are "decent" so long as

        L_max(N) < log(tweeted hashtags) /  log(distinct hashtags)

    where L_max(N) is the maximum length (in time) of words we want to
    look at probabilities for. Since we just want hashtag distributions,
    this is L=1. So we need the log ratio to be greater than 1, preferably
    by a lot.

    See page 5 of http://arxiv.org/pdf/cs/0406011v1.pdf.

    """
    db = twitterproj.connect()
    collkeys = [
        [twitterproj.hashtag_counts__states, 'fips'],
        [twitterproj.hashtag_counts__counties, 'geoid'],
        [twitterproj.hashtag_counts__squares, '_id'],
    ]
    dicts = [{}, {}, {}]

    for i, (coll, key) in enumerate(collkeys):
        print(coll)
        ratios = []
        for region in coll(db, bot_filtered=True):
            region_id = region[key]
            if len(region['counts']):
                total = np.sum(region['counts'].values())
                # Add 1 to prevent -inf ratios.
                denom = np.log2(1 + len(region['counts']))
                ratio = np.log2(total) / denom
                ratios.append(ratio)
            else:
                ratio = "NaN"

            dicts[i][region_id] = ratio

        ratios = np.array(ratios)
        dicts[i]['norm'] = np.nansum(ratios)
        dicts[i]['min'] = np.nanmin(ratios)
        dicts[i]['max'] = np.nanmax(ratios)

    filenames = [
        'json/grids.states.bot_filtered.countratio.json',
        'json/grids.counties.bot_filtered.countratio.json',
        'json/grids.squares.bot_filtered.countratio.json',
    ]

    for i, (fn, data) in enumerate(zip(filenames, dicts)):
        with open(fn, 'w') as f:
            json.dump(data, f)

def low_count_ratio(n=10, collection=None, hashtags=None, suffix=''):
    """
    Ratio of distinct hashtags with counts less than `n`.

    """
    db = twitterproj.connect()

    if hashtags is not None:
        hashtags = set(hashtags)

    collindex = {
        'states': [twitterproj.hashtag_counts__states, 'fips'],
        'counties': [twitterproj.hashtag_counts__counties, 'geoid'],
        'squares': [twitterproj.hashtag_counts__squares, '_id'],
    }
    if collection is None:
        collections = collindex.values()
    else:
        collections = [collindex[collection]]

    dicts = [{}, {}, {}]
    for i, (coll, key) in enumerate(collections):
        print(coll)
        ratios = []
        for region in coll(db, bot_filtered=True):
            region_id = region[key]
            if len(region['counts']):
                if hashtags is None:
                    low = sum([1 for ht, c in region['counts'].items()
                               if c < n])
                    norm = len(region['counts'])
                else:
                    low = sum([1 for ht, c in region['counts'].items()
                               if c < n and ht in hashtags])
                    norm = len([ht for ht in region['counts'] if ht in hashtags])
                ratio = float(low) / norm
                ratios.append(ratio)
            else:
                ratio = "NaN"

            dicts[i][region_id] = ratio

        ratios = np.array(ratios)
        dicts[i]['norm'] = np.nansum(ratios)
        dicts[i]['min'] = np.nanmin(ratios)
        dicts[i]['max'] = np.nanmax(ratios)

    filenames = OrderedDict([
        ('states', 'json/grids.states.bot_filtered.low{}_countratio{}.json'),
        ('counties', 'json/grids.counties.bot_filtered.low{}_countratio{}.json'),
        ('squares', 'json/grids.squares.bot_filtered.low{}_countratio{}.json'),
    ])

    if len(collections) == 1:
        filenames = [filenames[collection]]
        dicts = [dicts[0]]
    else:
        filenames = filenames.values()

    for i, (fn, data) in enumerate(zip(filenames, dicts)):
        fn = fn.format(n, suffix)
        with open(fn, 'w') as f:
            json.dump(data, f)

def make_all():
    names()
    population()
    landarea()
    hashtags_and_entropy()
    top5000ratios()
    unionX()
    users()
    countratio()
    low_count_ratio()

def get_hashtags_from_file(filename):
    hashtags = pandas.read_csv(filename)
    hashtags = list(hashtags['# hashtag'])
    return hashtags



if __name__ == '__main__':
    #top5000ratios()
    #users()
    low_count_ratio(2, collection='counties')
    #unionXY()
    """
    fileno = [14,16,18]
    filefamily = ['states','counties','squares']
    basename = 'unionX/unionX_{}_{}.txt'
    for number in fileno:
        for family in filefamily:
            filename = basename.format(family, number)
            hashtags = get_hashtags_from_file(filename)
            low_count_ratio(
                n=2,
                collection=family,
                hashtags=hashtags,
                suffix='_unionX{}'.format(number)
            )
            print(number,family)

    """

