"""
Get the outline of the USA, using county information.

"""
from __future__ import print_function

import fiona
import shapely
from shapely.ops import unary_union
from shapely.geometry import asShape, box, mapping
from descartes.patch import PolygonPatch
import matplotlib.pyplot as plt
import us
from collections import OrderedDict

import numpy as np
from itertools import product
import twitterproj

USA = box(-124.7625, 24.5210, -66.9326, 49.3845) # contiguous

def build_sparse_grid(tweet_collection,
                      grid_collection,
                      skip_users=True,
                      drop=True):
    """
    Takes about 30 minutes.

    """
    if drop:
        grid_collection.drop()

    hci = twitterproj.hashtag_counts_in
    if skip_users:
        uids = twitterproj.subcollections.get_skip_users()
    else:
        uids = None

    for cell in us_grid():
        counts, skipped = hci(tweet_collection, cell, uids)
        doc = OrderedDict()
        doc['geometry'] = mapping(cell)
        doc['counts'] = counts
        try:
            grid_collection.insert(doc)
        except pymongo.errors.DocumentTooLarge:
            # Hack for too large...probably won't happen.
            # Split in two...must be careful to join when querying.
            del doc['counts']
            del doc['_id']
            doc2 = doc.copy()
            items = counts.items()
            L = len(items)/2
            counts1 = dict(items[:L])
            counts2 = dict(items[L:])
            doc['counts'] = counts1
            doc2['counts'] = counts2
            grid_collection.insert(doc)
            grid_collection.insert(doc2)


def us_grid(resolution=.5, sparse=True):
    long_points = np.linspace(-125, -66, (-66 - -125)/resolution + 1)
    lat_points = np.linspace(24.5, 49.5, (49.5 - 24.5)/resolution + 1)

    outline = contiguous_outline2('../tiger/cb_2013_us_nation_20m.shp')

    for i, (xi, yi) in enumerate(product(range(len(long_points)-1),
                                         range(len(lat_points)-1))):
        cell = box(long_points[xi], lat_points[yi],
                   long_points[xi+1], lat_points[yi+1])
        if sparse:
            # Add cell only if it intersects contiguous USA
            if cell.intersects(outline):
                yield cell
        else:
            yield cell

def contiguous_outline(countyshp):
    fips = set([state.fips for state in us.STATES_CONTIGUOUS])
    shapes = []
    with fiona.open(countyshp) as f:
        for county in f:
            statefp = county['properties']['STATEFP']
            if statefp in fips:
                shapes.append(asShape(county['geometry']))

    outline = unary_union(shapes)
    return outline

def contiguous_outline2(usshp):
    with fiona.open(usshp) as f:
        outline = f[0]
    return asShape(outline['geometry'])

def plot_coords(ob, color='#999999', ax=None):
    if ax is None:
        ax = plt.gca()
    x, y = ob.xy
    ax.plot(x, y, '-', color=color, zorder=1)

def plot_outline(outline, patch=True, ax=None):
    if ax is None:
        ax = plt.gca()
    for polygon in outline:
        plot_coords(polygon.exterior, ax=ax)
        if patch:
            patch = PolygonPatch(polygon, facecolor='#FF0000', edgecolor='#0000FF',
                                 alpha=.5, zorder=2)
            ax.add_patch(patch)

def plot_us_outline(patch=True):
    #outline = contiguous_outline('../tiger/tl_2014_us_county.shp')
    outline = contiguous_outline2('../tiger/cb_2013_us_nation_20m.shp')
    f, ax = plt.subplots()
    plot_outline(outline, patch=patch)
    plot_coords(USA.exterior, color='#FF0000', ax=ax)
    grid = us_grid()
    for cell in grid:
        plot_coords(cell.exterior, color='#FFCC00', ax=ax)
    f.savefig('usoutline.pdf')

if __name__ == '__main__':
    #plot_us_outline(patch=False)
    #grid = list(us_grid(sparse=True))
    db = twitterproj.connect()
    build_sparse_grid(db.tweets.with_hashtags, db.grids.squares.bot_filtered, skip_users=True)
    build_sparse_grid(db.tweets.with_hashtags, db.grids.squares, skip_users=False)
