#!/usr/bin/env python3

# Run with: spark-submit train.py

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StopWordsRemover, Tokenizer, Word2Vec
from pyspark.sql import Row, SparkSession
import csv, re

spark = SparkSession \
    .builder \
    .appName("Sentiment Analysis") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

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

def transform_row(row):
    lbl = int(row[1])
    text = parse_tweet(row[3])
    return Row(label=lbl, sentence=text)

def get_final_data(filename):
    # Get raw training data
    # Note: header does not exist
    raw_data = spark.sparkContext.textFile(filename).mapPartitions(lambda x: csv.reader(x))
    # Put schema and make dataframe
    data = raw_data.map(lambda x: transform_row(x))
    dataDF = spark.createDataFrame(data)
    # Tokenize sentences to words
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    tokenized = tokenizer.transform(dataDF)
    # Remove Stop Words
    remover = StopWordsRemover(inputCol="words",outputCol="base_words")
    base_words = remover.transform(tokenized)
    # Get train data only
    train_data = base_words.select("base_words", "label")
    # Vectorize
    word2Vec = Word2Vec(vectorSize=3, minCount=0,inputCol="base_words",outputCol="features")
    model = word2Vec.fit(train_data)
    # Return final data
    return model.transform(train_data).select("label", "features")


if __name__ == '__main__':

    # Parse csv and vectorize words for train and test
    final_train_data = get_final_data("/data/train.csv")
    final_test_data = get_final_data("/data/test.csv")

    # Start Logistic Regression
    lr = LogisticRegression(maxIter=10, regParam=0.3,elasticNetParam=0.8)
    # Train
    lrModel = lr.fit(final_train_data)
    # Test
    predict = lrModel.transform(final_test_data)
    # Show
    predict.show()

    # TODO: Extract tweets from HIVE and Evaluate results
