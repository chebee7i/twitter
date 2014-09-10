Information on mongo/pymongo
============================

To find all documents that have a nonzero number of hashtags,
we can do:

    tweets.find({'hashtags.0': {$exists: 1})

We could also do the following, but note, it also returns elements that 
do not have a hashtags field. This is not an issue in our database since
we made sure each document had the field.

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

Mongodb failed and complained about memory issues. So avoided the 
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
So we remove the "tweets" field.

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
a has_hashtags field with sparse index: will be backed up to 
mongodb_2014-09-11

