#!/usr/bin/env python3


# Run with: spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 consumer.py
# Be sure to have: PYTHONIOENCODING=utf8

# Ingest twitter stream and dump it to Hive

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, HiveContext
import re
import json


sc = SparkContext(appName="Consumer")
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 10)

def parse_tweet(tweet):
    parsed = tweet
    # Remove url links
    parsed = re.sub(r"http\S+", "URL", parsed)
    # Remove emails
    parsed = re.sub(r"[\w\.-]+@[\w\.-]+", "EMAIL", parsed)
    # Remove twitter handles and hashtags
    parsed = re.sub(r"(@|#)\w+", "", parsed)
    # Remove html entities
    parsed = re.sub(r"&((l|g|quo)t|amp);", "", parsed)
    # Return parsed tweet
    return parsed.strip()

zkQuorum = 'kafka:2181'
# Create stream
kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, 'spark-streaming-consumer', {'tweets':1})
# Parse to JSON
parsed = kafkaStream.map(lambda t: json.loads(t[1]))
# Filter out retweets
parsed = parsed.filter(lambda t: 'retweeted_status' not in t)
# Extract Tweet messages (text) and parse
text = parsed.map(lambda tweet: parse_tweet(tweet['text']))
# TODO: Dump to Hive

print('-------------------------------------------------')

text.pprint()

ssc.start()
ssc.awaitTermination()
