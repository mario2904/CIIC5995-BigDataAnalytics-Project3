#!/usr/bin/env bash

# Initialize Hive
hdfs dfs -chmod g+w /tmp
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /user/hive/warehouse
schematool -dbType derby -initSchema

# Extract training data
unzip Sentiment-Analysis-Dataset.zip
# Change name
mv Sentiment\ Analysis\ Dataset.csv data.csv
# Remove header (first line)
sed -i '1d' data.csv
# Randomize data
shuf data.csv > shuffled_data.csv
# Training data 60K tweets
head -n 60000 shuffled_data.csv > train.csv
# Testd data 40K tweets
tail -n 40000 shuffled_data.csv > test.csv
# Push to HDFS
hdfs dfs -mkdir /data
hdfs dfs -put train.csv /data
hdfs dfs -put test.csv /data
