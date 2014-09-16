"""
Geospatial queries.

"""
from collections import defaultdict
from shapely.geometry import mapping

__all__ = [
    'hashtag_counts_in',
]

def hashtag_counts_in(collection, geometry, skip_users=None):
    """
    Returns hashtag counts for all tweets in a geometry.

    Parameters
    ----------
    collection : MongoDB collection
        The collection of tweets.
    geometry : GeoJSON-like geometry or list
        The GeoJSON-like object representing the desired geometry.
        This can be a Polygon or MultiPolygon.  For a list, the [polygon]_
        should be a list of LinearRing coordinate arrays. A LinearRing
        coordinate array is a list of (longitude, latitude) pairs that is
        closed---the first and last point must be the same.
    skip_users : list of int
        A list of Twitter user ids. Any tweet from these user ids will be
        skipped and not included in the counts.

    Returns
    -------
    counts : defaultdict
        The counts for each hashtag. Keys are hashtags, values are counts.
    skipped : int
        The number of tweets that were not counted, due to `skip_users`.

    Examples
    --------
    >>> seattle = [
    ...     [[-122.4596959,47.4810022],
    ...      [-122.4596959,47.7341388],
    ...      [-122.2244329,47.7341388],
    ...      [-122.2244329,47.4810022],
    ...      [-122.4596959,47.4810022]],
    ... ]
    ...
    >>> counts, skipped = hashtags_counts_in(collection, seattle)

    Notes
    -----
    This relies on MongoDB's geospatial queries.
    A '2dsphere' index on the collection will speed this up.
    For more information on geojson polygons:

    .. [polygon] http://geojson.org/geojson-spec.html#polygon

    """
    if skip_users is None:
        skip_users = set([])
    else:
        skip_users = set(skip_users)

    try:
        # Shapley Polygon or MultiPolygon to geoJSON-like object
        geometry = mapping(geometry)
    except AttributeError:
        pass

    if 'coordinates' not in geometry:
        # Use the list to define a polygon.
        geometry = {'type': 'Polygon', 'coordinates': geometry}

    c = collection.find({
        'coordinates': {
            '$geoWithin': {
                '$geometry' : geometry
            }
        }
    })

    counts = defaultdict(int)
    skipped = 0
    for tweet in c:
        if tweet['user']['id'] in skip_users:
            skipped += 1
        else:
            for hashtag in tweet['hashtags']:
                counts[hashtag] += 1

    return counts, skipped

