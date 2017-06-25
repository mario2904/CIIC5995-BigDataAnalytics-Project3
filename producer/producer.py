#!/usr/bin/env python3

# From Tutorial: https://www.dataquest.io/blog/streaming-data-python/
from time import localtime, strftime
from kafka import KafkaProducer
import tweepy
import json
# import logging
# logging.basicConfig(level=logging.DEBUG)

# Use Credentials
TWITTER_APP_KEY = ''
TWITTER_APP_SECRET = ''
TWITTER_KEY = ''
TWITTER_SECRET = ''

KAFKA_BROKER_URL = 'kafka:9092'
KAFKA_TOPIC = 'tweets'

auth = tweepy.OAuthHandler(TWITTER_APP_KEY, TWITTER_APP_SECRET)
auth.set_access_token(TWITTER_KEY, TWITTER_SECRET)

api = tweepy.API(auth)

# Connect to Kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL) #, api_version=(0,10))

class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        # Send Status text to Kafka
        # producer.send(KAFKA_TOPIC, status.text.encode())

        # Send raw JSON tweet to Kafka
        producer.send(KAFKA_TOPIC, json.dumps(status._json).encode())
        # Print raw JSON tweet to std out
        # print(json.dumps(status._json))

    def on_error(self, status_code):
        if status_code == 420:
            return False

stream_listener = StreamListener()
# https://stackoverflow.com/questions/23601634/how-to-restart-tweepy-script-in-case-of-error
while True:
    try:
        stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
        stream.filter(track=["maga", "dictator", "impeach", "drain", "swamp", "comey"])
    except KeyboardInterrupt:
        raise
    except:
        print(strftime("[%a, %d %b %Y %H:%M:%S]", localtime()), "Error in stream.")
        continue
