"""
Geospatial queries.

"""
from collections import defaultdict
from shapely.geometry import mapping

__all__ = [
    'polygon_hashtags',
]

def polygon_hashtags(collection, polygon, skip_users=None):
    """
    Returns hashtag counts for all tweets in a polygon.

    Parameters
    ----------
    collection : MongoDB collection
        The collection of tweets.
    polygon : Shapely Polygon, geoJSON Polygon, list
        The coordinates of the polygon. Note that for a list, it should be a
        list of LinearRing coordinate arrays. A LinearRing coordinate array
        is list of (longitude, latitude) pairs that is closed---the first and
        last point must be the same).
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
    >>> counts, skipped = polygon_hashtags(collection, seattle)

    Notes
    -----
    This relies on MongoDB's geospatial queries.
    A '2dsphere' index on the collection will speed this up.

    """
    if skip_users is None:
        skip_users = set([])
    else:
        skip_users = set(skip_users)

    try:
        # Shapley Polygon to geoJSON-like object
        coords = mapping(polygon)
    except AttributeError:
        pass

    if 'coordinates' in polygon:
        # A geoJSON-like object
        coords = polygon['coordinates']
    else:
        # Use the polygon as a list of coordinates
        coords = polygon

    c = collection.find({
        'coordinates': {
            '$geoWithin': {
                '$geometry' : {
                    'type': 'Polygon',
                    'coordinates': coords
                }
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

