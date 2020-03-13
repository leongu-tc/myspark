#!/usr/bin/env bash

cd cleared_stock_daily

/usr/hdp/2.2.0.0-2041/spark24/bin/spark-submit \
 --conf "spark.driver.extraLibraryPath=/usr/hdp/2.2.0.0-2041/hadoop/lib/native/" \
 --class "leongu.myspark._business.clearedstock.ClearedStock" \
 --master yarn \
 --deploy-mode client \
 --driver-memory 1g \
 --executor-memory 2g \
 --executor-cores 1 \
 --num-executors 30 \
 --files sync.yaml \
 --queue assetanalysis \
 myspark-1.0.0-jar-with-dependencies.jar \
 sync.yaml