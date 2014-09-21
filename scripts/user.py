"""
Simple script to fetch all hashtags for a user, by user id.

"""
import twitterproj

def user_tweets(userid, collection):
    klass = twitterproj.Tweet
    for tweet in collection.find({'user.id': userid}, as_class=klass):
        yield tweet

if __name__ == '__main__':
    db = twitterproj.connect()
    for tweet in user_tweets(34244261, db.tweets.with_hashtags):
        print(','.join(tweet['hashtags']))


