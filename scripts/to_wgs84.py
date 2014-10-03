"""
Python conversion of coordinate systems.

Intended use: NAD83 -> WGS84

See Also:
    https://github.com/Toblerity/Fiona/blob/master/examples/with-pyproj.py

The standard way to do this would be to use ogr2ogr:

    $ ogr2ogr -t_srs "EPSG:4326" output.shp input.shp

"""
from __future__ import print_function

import sys

from pyproj import Proj, transform

import fiona
from fiona.crs import from_epsg

def to_crs(old, new, new_epsg=4326):
    """Convert old shapefile to new shapefile with new crs.

    """
    crs = from_epsg(new_epsg)

    with fiona.open(old, 'r') as source:
        sink_schema = source.schema.copy()
        p_in = Proj(source.crs)
        with fiona.open(
                new, 'w',
                crs=crs,
                driver=source.driver,
                schema=sink_schema,
                ) as sink:

            p_out = Proj(sink.crs)

            for f in source:

                try:
                    assert f['geometry']['type'] == "Polygon"
                    new_coords = []
                    for ring in f['geometry']['coordinates']:
                        x2, y2 = transform(p_in, p_out, *zip(*ring))
                        new_coords.append(zip(x2, y2))
                    f['geometry']['coordinates'] = new_coords
                    sink.write(f)

                except Exception, e:
                    # In practice, this won't work for most shapes since they
                    # are frequently of type MultiPolygon.
                    print("Error transforming feature {}".format(f['id']))
                    raise


if __name__ == '__main__':
    to_crs('../tiger/cb_2013_us_nation_20m.shp', 'new.shp')
