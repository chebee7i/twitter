"""
Builds temporal data from all tweets for each county.

"""

from itertools import islice
from tzwhere.tzwhere import tzwhere
import twitterproj
from collections import defaultdict, OrderedDict
import json
import datetime
import fiona
import us
import pytz

utc = pytz.utc

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))

def get_tweetlocal(tweet):
    dt = tweet['created_at']
    tz = z.tzNameAt(tweet['coordinates'][1], tweet['coordinates'][0])
    # Note, we are still returning UTC time.
    return utc.localize(dt), tz

db = twitterproj.connect()
z = tzwhere()

county_shp = '../tiger/tl_2014_us_county.shp'
class JobManager(twitterproj.JobManager):
    def parent(self):
        with fiona.open(county_shp, 'r') as f:
            for i, feature in enumerate(f):
                self.launch_child(i, feature, wait=60)

    def child(self, job_id, feature):
        state_fips = feature['properties']['STATEFP']
        county_fips = feature['properties']['COUNTYFP']
        geoid = feature['properties']['GEOID']
        name = feature['properties']['NAMELSAD']

        mapping = us.states.mapping('fips', 'name')
        state = mapping[state_fips]
        m = u"Job {0}: {1}, {2}\t\t{3}".format(job_id, name, state, geoid)
        print(m.encode('utf-8'))
        fips = set([state.fips for state in us.STATES_CONTIGUOUS])

        if state_fips not in fips:
            # Consider only counties in the contiguous US.
            return

        chunksize = 100000
        db = twitterproj.connect()
        region = feature['geometry']
        tweets = twitterproj.tweets_in_region(db.tweets, region)

        # Every tweet in the same county has the same timezone.
        timezone = None
        docs = defaultdict(lambda: OrderedDict()) # 5 min intervals

        while True:
            chunk = list(take(chunksize, tweets))
            if not chunk:
                break

            # Get the timezone
            for tweet in chunk:
                # Some GPS coordintes don't seem to give a timezone.
                # E.g. maybe too far out in ocean combined with a bug.
                _, timezone_str = get_tweetlocal(tweet)
                if timezone_str:
                    timezone = pytz.timezone(timezone_str)
                    break
            else:
                raise Exception("Could not get timezone from any tweet.")

            for tweet in chunk:

                dt = utc.localize(tweet['created_at'])
                # Now create the local time.
                dt = dt.astimezone(timezone)
                key = datetime.datetime(dt.year, dt.month, dt.day)

                doc = docs[key]
                if not len(doc):
                    doc['state_fips'] = state_fips
                    doc['county_fips'] = county_fips
                    doc['geoid'] = geoid
                    doc['timezone'] = timezone_str
                    doc['date'] = key
                    doc['weekday'] = key.weekday()
                    doc['year'] = key.year
                    doc['month'] = key.month
                    doc['day'] = key.day
                    doc['counts'] = [0] * 24 * 12 # 5 minute intervals

                idx = 12 * dt.hour + dt.minute // 5
                doc['counts'][idx] += 1


        db = twitterproj.connect()
        docs = iter(docs.values())
        chunksize = 1000
        collection = db.grids.temporal.counties
        while True:
            chunk = take(chunksize, docs)
            if not chunk:
                break

            try:
                collection.insert(chunk)
            except pymongo.errors.DocumentTooLarge:
                # This shouldn't happen. Fail hard.
                raise


if __name__ == '__main__':
    jm = JobManager('tweetlocal.db', nProc=22)
    jm.main()

