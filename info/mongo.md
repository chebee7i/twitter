Information on mongo/pymongo
============================

The database is called "twitter". Connect to it as follows:

```bash
    $ mongo twitter
```

The current collections are:

```js
    > db.getCollectionNames()
    [
            "hashtagCounts.byState",
            "hashtagTweets",
            "hashtags",
            "hashtags.botFiltered",
            "sources",
            "system.indexes",
            "tweets",
            "userHashtagTweetCount",
            "userTweetCount"
    ]
```

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

