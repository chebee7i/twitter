"""
Script to build a shapefile for the squares grid.

"""

from collections import OrderedDict
import fiona
from shapely.geometry import mapping, Polygon
import twitterproj


driver = 'ESRI Shapefile'
schema = OrderedDict([('geometry', 'Polygon'),
                      ('properties', {'id': 'int'})])

db = twitterproj.connect()
shpfile = 'data/grids.squares.shp'
with fiona.open(shpfile, 'w', driver, schema) as f:
    for i, square in enumerate(db.grids.squares.find()):
        geometry = square['geometry']
        properties = {'id': int(square['_id'])}
        feature = OrderedDict([('geometry', geometry),
                               ('properties', properties)])
        f.write(feature)

