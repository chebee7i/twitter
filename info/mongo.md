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




