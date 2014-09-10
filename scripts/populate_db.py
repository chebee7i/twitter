"""
Populates the local mongo database with tweets using the gzip
archives in the `data` directory.

"""
import os

from twitterproj.helpers import populate_db

datadir = os.path.abspath('../data')
populate_db(datadir, dry_run=True)
