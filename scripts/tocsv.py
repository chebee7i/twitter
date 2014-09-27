"""
Script to write hashtag counts on grid to CSV.

"""
from __future__ import unicode_literals
from __future__ import print_function

import errno
import io
import os

import pymongo
import twitterproj
from shapely.geometry import asShape

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def csv_states(filename, bot_filtered=True):
    """
    Writes the states to file.

    """
    db = twitterproj.connect()
    template = "{},{},{}"
    with io.open(filename, 'w', encoding='utf-8') as f:
        f.write("# state name, state abbrev, fips\n")
        lines = []
        for state in twitterproj.hashtag_counts__states(db, bot_filtered=bot_filtered):
            line = template.format(state['name'],
                                   state['abbrev'],
                                   state['fips'])
            lines.append(line)
        f.write('\n'.join(lines))

def csv_counties(filename, bot_filtered=True):
    """
    Write the counties to file.

    """
    db = twitterproj.connect()
    template = "{},{},{}"
    with io.open(filename, 'w', encoding='utf-8') as f:
        f.write("# county name, state_fips, county_fips\n")
        lines = []
        for state in twitterproj.hashtag_counts__counties(db, bot_filtered=bot_filtered):
            line = template.format(state['name'],
                                   state['state_fips'],
                                   state['county_fips'])
            lines.append(line)
        f.write('\n'.join(lines))

def csv_squares(filename, bot_filtered=True):
    """
    Write the squares to file.

    """
    db = twitterproj.connect()
    if bot_filtered:
        collection = db.grids.squares.bot_filtered
    else:
        collection = db.grids.squares

    template = "{},{},{},{}"
    with io.open(filename, 'w', encoding='utf-8') as f:
        f.write("# min longitude, min latitude, max longitude, max latitude\n")
        lines = []
        for square in collection.find():
            poly = asShape(square['geometry'])
            line = template.format(*poly.bounds)
            lines.append(line)
        f.write('\n'.join(lines))

def counts_csv(what, hashtags, filename, bot_filtered=True):
    assert(what in ['states', 'counties', 'squares'])

    db = twitterproj.connect()

    if what == 'states':
        it = twitterproj.hashtag_counts__states(db, bot_filtered=bot_filtered)
    elif what == 'counties':
        it = twitterproj.hashtag_counts__counties(db, bot_filtered=bot_filtered)
    elif what == 'squares':
        if bot_filtered:
            it = db.grids.squares.bot_filtered.find()
        else:
            it = db.grids.squares.find()

    lines = []
    for region in it:
        counts = region['counts']
        cvals = [counts.get(h, 0) for h in hashtags]
        lines.append(','.join(map(str,cvals)))

    with io.open(filename, 'w', encoding='utf-8') as f:
        f.write('# rows are {}, columns are hashtags, values are counts\n'.format(what))
        f.write('\n'.join(lines))

def writeall_csv():
    mkdir_p('grids')

    for bot_filtered, tail in [(True, '.bot_filtered.csv'), (False, '.csv')]:
        hashtags = twitterproj.sorted_hashtags(bot_filtered, 'grids/hashtags' + tail)[:5000]
        counts_csv('states', hashtags, 'grids/states_hashtag_counts' + tail, bot_filtered)
        counts_csv('counties', hashtags, 'grids/counties_hashtag_counts' + tail, bot_filtered)
        counts_csv('squares', hashtags, 'grids/squares_hashtag_counts' + tail, bot_filtered)
        pass

    tail = '.csv'
    csv_states('grids/states' + tail)
    csv_counties('grids/counties' + tail)
    csv_squares('grids/squares' + tail)


