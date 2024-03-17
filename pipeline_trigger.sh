#!/bin/bash

echo "----------------------Start Producing Weather Data--------------------------------"
jupyter nbconvert --execute --to notebook --inplace producer.ipynb &

echo "---------------------Start Streaming Data To Hive & HDFS---------------------------"
jupyter nbconvert --execute --to notebook --inplace consumer.ipynb &
