Information on MongoDB and pymongo
==================================

The database is called "twitter". Connect to it as follows:

```bash
$ mongo twitter
```

The current collections are:

```js
> db.getCollectionNames()
[
        "grid.counties",
        "grid.counties.bot_filtered",
        "grid.states",
        "grid.states.bot_filtered",
        "hashtags",
        "hashtags.bot_filtered",
        "hashtags.flagged",
        "sources",
        "system.indexes",
        "tweets",
        "tweets.with_hashtags",
        "users",
        "users.flagged",
        "users.with_hashtags"
]
```

The primary collections are:

    "tweets"
    "sources"

All others are derivative, but still very useful, collections. The
"tweets" collection  holds all tweets under consideration for the
project (geocoded tweets within a bounding box that includes the
contiguous states). The "sources" collection lists the source
files from which the tweets were extracted.

["system.indexes" is an administrative collection that MongoDB uses.]

Generally, collection names will be hierarchical. The first level states what
the collection is about. So "tweets.with_hashtags" is a collection of tweets
that have hashtags, and so on. Some collections are temporary and will need
to be regenerated. A description of each is below.

- *grids.counties* - Each document is a "county" (county, parish, borough, etc)
  with the following properties: "state_fips", "county_fips", "landarea",
  "name", "counts", "geoid". Attributes "state_fips" and "county_fips" are the
  FIPS code for the state and count. "geoid" is the combined 5-digit string of
  the state and county FIPS code. "landarea" is the area of land within the
  county.

- *grids.counties.bot_filtered* - Same as *grids.counties*, but tweets are not
  included in the counting if their user id appears in the *users.flagged*
  collection and if the user has the "avoid" property being true.

- *grids.states* - Each document is a "state". Each document has the following
  properties: "name", "counts", "fips", "abbrev", "landarea". Attributes "name"
  and "abbrev" are the full name and abbreviation of the state. "fips" is the
  FIPS code associated to the state. "counts" is an object mapping hashtags
  to the number of times the hashtag appeared in tweets taken from the
  "tweets.with_hashtags" collection. "landarea" is the area of land within the
  state. The documents were populated using the Python package "us" which
  includes territories. So there are a total of 56 documents---which includes
  all 50 states, DC and 5 territories. Note however, that there are 57 documents
  in the collection. This is because California could not fit within the MongoDB
  16 MiB document restriction. So it was partitioned into two documents.
  Care should be taken at the MongoDB command line when working with CA.
  In the Python library, the function 'get_hashtag_counts' will merge
  the documents for you.

- *grids.states.bot_filtered* - Same as *grids.states*, but tweets are not
  included in the counting if their user id appears in *users.flagged*
  collection and if the user has the "avoid" property being true.

- *hashtags* - Each document is a hashtag that appears in some tweet. The
  "count" property lists the number of tweets that mentioned the hashtag.

- *hashtags.bot_filtered* - Each document is a hashtag that appears in some
  tweet. The "count" property lists the number of tweets from users not
  appearing in the "users.flagged" collection (that are marked "avoid")
  that have mentioned the hashtag.

- *hashtags.flagged* - Each document is a hashtag that has been flagged
  as one that we might want to avoid in the analysis. The "autobot"
  property states whether the hashtag was used to flag users. Those users
  will appear in "users.flagged" and have the property "by_hashtag" set to
  true. The property "avoid" is set to true only for those hashtags that
  should not be included in the analysis.

- *sources* - Each document contains a source file from which tweets
  were extracted.

- *tweets* - Each document is a tweet, containing a subset of all the
  information that Twitter provides. The raw data (found in the files
  listed under *sources*) contains all the tweet data. There is a sparse
  index on tweets that have hashtags.
- *tweets.with_hashtags* - Each document is a tweet that has at least
  one hashtag. Since only about 10% of geocoded tweets had hashtags,
  it made sense to extract these.

- *users* - Each document is a user (represented by their twitter ID)
  and there is a "count" property that lists the number of times the user
  tweeted.

- *users.flagged* - Each document is a user that has been flagged as
  one that we might want to ignore during analysis. There are various
  properties that specify how the user was identified. "first_1000" if true,
  means the user was manually flagged from the 1000 most active users
  (active according to counts from the *users* collection). "avoid"
  means that any "bot_filtered" collection will not contain data from
  such users. "by_hashtag" means that the user was flagged because they
  made use of a flagged hashtag.

- *users.with_hashtags* - Each document is a user (represented by their
  Twitter ID) and there is a "count" property that lists the number of times
  the user tweeted with a hashtag.


Flagged Users
=============
Not all Twitter users are humans;, and including their hashtagged tweets may
adversely affect analysis. We used a variety of techniques to identify
"bots". The "users.flagged" table contains our list of flagged users. If
"avoid" is True for any user, then our analysis does not include any tweets
from that user.

First, we sorted users by tweet count. Then the first 1000 users on that list
were manually checked for "bot" status. The reason for sorting by tweet count
is that many bots tweet more frequently than humans. 364 bots were identified
and they all have the "first_1000" property set to True, as well as "avoid"
set to True.

Then, we sorted hashtags by how frequently they were tweeted. From this list,
we identified hashtags that were likely to have been tweeted primarily by bots.
Hashtags were labeled: "bad" or "suspicious" or were not labeled. Users who had
used any of those flagged hashtags were flagged as potentially a bot with the
"by_hashtag" property---note, some of those users had already been identified
as bots (243 of the 364 from the "first_1000", in fact). Of the potential bots,
the 1000 most active of them (according to tweet frequency) that were not
already marked as "avoid" were manually checked. This revealed an additional
178 robots, of which 157 were users who had tweeted a "bad" hashtag. All 178
of these have the "flagged_1000" property set to True.

Finally, since most of the users who tweeted a "bad" hashtag have turned out
to be bots, we decided to mark all users who used a "bad" hashtag as
avoid=True. All of these users now have the "bad" property as True. There are
2059 such users, of which 1659 had not been previously declared to be rebots.

Other information
=================


To find all documents that have a nonzero number of hashtags,
we can do:

    tweets.find({'hashtags.0': {$exists: 1}})

We could also do the following, but note, it also returns elements that
do not have a hashtags field. This is not an issue in our database since
we made sure each document had the field. It also seems to be slower.

    tweets.find({hashtags: {$not: {$size: 0}}})

To find the most popular hashtags:

    hashtags.find().sort({count: -1})                 # mongo
    hashtags.find().sort('count', pymongo.DESCENDING) # pymongo

Mongo Database
--------------
Locally, we built a mongodb database called 'twitter'.
There were two collections:

    sources : Holds the names of every file from which tweets were inserted.
    tweets  : Holds the actual tweets.

The format of each document in specified in `twitterproj/helpers.py`.
Roughly, this was 4 months of data. Each day had 3 source files, with each
source file being about 2 GiB gzipped (and 11 GiB gunzipped). Of those
tweets, only continential USA tweets with GPS coordinates were added to
the database. This totaled almost 400 million tweets.

After building the database, we wanted a collection of hashtags.
Only about 10% of the tweets had hashtags. So we thought we could store
an element which contained the tweet IDs mentioning each hashtag.

    $ mongo twitter
    > db.tweets.aggregate([
        {$unwind: "$hashtags"},
        {$group: {
            _id:"$hashtags",
            count: {$sum: 1},
            tweets: {$push: "$_id"}
        }},
        {$out: "hashtags"}
    ])

Mongodb failed and complained about memory issues. So we avoided the
aggregation memory limit and allowed it to use disk space.

    > db.runCommand(
        { aggregate: "tweets",
          pipeline: [
            {$unwind: "$hashtags"},
            {$group: {
                _id:"$hashtags",
                count: {$sum: 1},
                tweets: {$push: "$_id"} // needs to be removed
            }},
            {$out: "hashtags"}
          ],
          allowDiskUse: true
        }
    )

But this still failed since mongodb has a hard limit of 16 MiB per document.
To store all tweets mentioning a particular hashtag would be too large.
So we removed the "tweets" field.

We also created an index for the counts.
-1 for descending from high to low.

    db.hashtags.ensureIndex({count:-1})

Hashtags were originally stored case-sensitively since Twitter displays
them sensitively (and that is how we received them). But Twitter treats
hashtags case-insensitively for classification purposes. So we decided to
convert each hashtag to lowercase. [The parser/insert code was updated
to convert to lowercase at first entry.]

    db.tweets.find().forEach(function(tweet) {
        tweet.hashtags = tweet.hashtags.map(function (tweet) {
            return tweet.toLowerCase();
        });
        db.tweets.save(tweet);
    })

This takes about 5 days. Once complete, we will create a new field and
then a sparse index.

    db.tweets.update(
        {'hashtags.0': {$exists: true},
        {$set: {has_hashtags: true}},
        {multi: true}
    )
    db.tweets.ensureIndex({'has_hashtags':1}, {sparse: true})

The db with lowercased hashtags, a hashtags collection with index on count,
a has_hashtags field with sparse index...was backed up to
mongodb_2014-09-13.

    > db.runCommand(
        { aggregate: "tweets",
          pipeline: [
            {$match: {has_hashtags:true}},
            {$group: {
                _id:"$user.id",
                count: {$sum: 1},
            }},
            {$out: "userHashtagTweetCount"}
          ],
          allowDiskUse: true
        }
    )

Created more tables...

    > db.runCommand({aggregate:"tweets", pipeline: [{$group:{_id:"$user.id",count:{$sum:1},}},{$out:"userTweetCount"}],allowDiskUse:true})

    > db.userTweetCount.count()
    3933010
    > db.userTweetCount.ensureIndex({count:-1})

    > db.runCommand({aggregate:"tweets", pipeline: [{$match:{has_hashtags:true}},{$group:{_id:"$user.id",count:{$sum:1},}},{$out:"userHashtagTweetCount"}],allowDiskUse:true})

    > db.userHashtagTweetCount.ensureIndex({count:-1})

Pulling out hashtagged tweets into their own collection.

    > db.runCommand({aggregate:"tweets", pipeline: [{$match:{has_hashtags:true}},{$out:"hashtagTweets"}], allowDiskUse:true})
    > db.hashtagTweets.ensureIndex({created_at: 1})

Now we want to be able to query using MongoDBs geospatial tools.
So we will create a 2dsphere index. We might also be interested in
seeing location data by date. So we create a compound index for that.

    > db.hashtagTweets.ensureIndex({coordinates: "2dsphere"})
    > db.hashtagTweets.ensureIndex({coordinates: "2dsphere", created_at: 1})

We have identified a set of users we would like to ignore, since
they appear to be robots. We will do another hashtag aggregation that
ignores these users. Since we have extracted hashtagged tweets already,
we can use this smaller table.


    > db.runCommand({
         aggregate: "hashtagTweets",
         pipeline: [{$match: {"user.id" : {$ni : [LIST OF IDS]}}},
                    {$unwind: "$hashtags"},
                    {$group: {_id:"$hashtags", count: {$sum:1}}},
                    {$out: "hashtags.botFiltered"}],
         allowDiskUse: true
    })

However, getting this list of IDs into the mongo terminal is a pain.
So we do this with a script: botFiltered.py

