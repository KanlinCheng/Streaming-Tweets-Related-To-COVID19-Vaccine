import json
import tweepy
import pandas as pd
import datetime
import csv
import numpy as np
import pprint
import time


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

    def __init__(self, time_limit=300):
        self.start_time = time.time()
        self.limit = time_limit
        super(MyStreamListener, self).__init__()

    def on_connect(self):
        print("Connected to Twitter API.")

    def on_status(self, status):

        # Tweet ID
        tweet_id = status.id

        # User ID
        user_id = status.user.id
        # Username
        username = status.user.name

        # Tweet
        if status.truncated == True:
            tweet = status.extended_tweet['full_text']
            hashtags = status.extended_tweet['entities']['hashtags']
        else:
            tweet = status.text
            hashtags = status.entities['hashtags']

        # Read hastags
        hashtags = read_hashtags(hashtags)

        # Retweet count
        retweet_count = status.retweet_count
        # Language
        lang = status.lang

        # If tweet is not a retweet and tweet is in English
        if not hasattr(status, "retweeted_status") and lang == "en":
            self.tweets.append(status)
            print(user_id, username, tweet_id, tweet, retweet_count, hashtags)
            # Connect to database
            # dbConnect(user_id, username, tweet_id, tweet, retweet_count, hashtags)

        if (time.time() - self.start_time) > self.limit:
            print(time.time(), self.start_time, self.limit)
            return False

    def on_error(self, status_code):
        if status_code == 420:
            # Returning False in on_data disconnects the stream
            return False


if __name__ == "__main__":
    api = auth_tweepy_kevin()

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener,
                             tweet_mode="extended")
    myStream.filter(track=['COVAX', 'GetTheShot', 'vaccine'])

