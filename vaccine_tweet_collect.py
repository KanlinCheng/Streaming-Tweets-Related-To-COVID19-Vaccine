import tweepy
import time
import pymongo
import json
import pandas as pd
import datetime


def auth_tweepy_kevin():
    auth = tweepy.OAuthHandler("",
                               "")
    auth.set_access_token("",
                          "")
    api = tweepy.API(auth)
    return api


def read_hashtags(tag_list):
    hashtags = []
    for tag in tag_list:
        hashtags.append(tag['text'])
    return hashtags


class MyStreamListener(tweepy.StreamListener):
    tweets = []
    target_hashtags_list = list()
    with open("vaccine-hashtags.txt", 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            line = line.lower()
            target_hashtags_list.append(line)
    print("Hashtags list")
    print(target_hashtags_list)

    def __init__(self, time_limit=300):
        self.start_time = time.time()
        self.limit = time_limit
        super(MyStreamListener, self).__init__()

    def on_connect(self):
        print("Connected to Twitter API.")

    def on_status(self, status):
        client = pymongo.MongoClient(
            "mongodb+srv://user:dsci551@cluster0.prlkd.mongodb.net/twitter_db?retryWrites=true&w=majority")
        db = client.twitter_db

        tweet_id = status.id    # Tweet ID
        user_id = status.user.id    # User ID
        username = status.user.name    # Username
        create_time = status.created_at  # create_time
        lang = status.lang    # Language
        retweet_count = status.retweet_count    # Retweet count, would not be stored as the value is always 0

        # Place info
        place = None
        country = None
        country_code = None
        city = None
        if hasattr(status, "place"):
            place = status.place
        if place is not None:
            country = str(place.country)
            country_code = str(place.country_code)
            city = str(place.name)

        # Tweet
        if status.truncated:
            tweet = status.extended_tweet['full_text']
            hashtags = status.extended_tweet['entities']['hashtags']
        else:
            tweet = status.text
            hashtags = status.entities['hashtags']

        # Read hastags
        hashtags = read_hashtags(hashtags)

        # intersection of obtained hashtags and target hashtags
        hashtags_lower = [h.lower() for h in hashtags]
        vaccine_str = "vaccine"
        hashtag_intersection = [hashlow for hashlow in hashtags_lower if vaccine_str in hashlow or hashlow in self.target_hashtags_list]

        # If tweet is not a retweet and tweet is in English and has a required hashtag
        if lang == "en" and (len(hashtag_intersection) > 0 and not hasattr(status, "retweeted_status")):
            self.tweets.append(status)
            print(tweet, hashtags)
            data = {
                'tweet_id': tweet_id,
                'created_at': create_time,
                'country': country,
                'country_code': country_code,
                'city': city,
                'user_id': user_id,
                'username': username,
                'tweet': tweet,
                'hashtags': hashtags_lower
            }

            try:
                # insert the data into the mongoDB into a collection called twitter_stream
                # if twitter_stream doesn't exist, it will be created.
                db.twitter_stream.insert(data)
            except Exception as e:
                print(e)

        if (time.time() - self.start_time) > self.limit:
            print(time.time(), self.start_time, self.limit)
            return False

    def on_error(self, status_code):
        if status_code == 420:
            # Returning False in on_data disconnects the stream
            return False


if __name__ == "__main__":
    api = auth_tweepy_kevin()

    target_hashtags_list = list()
    with open("vaccine-hashtags.txt", 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            target_hashtags_list.append(line)

    languages = ['en']

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener,
                             tweet_mode="extended", wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    myStream.filter(languages=languages, track=target_hashtags_list)

