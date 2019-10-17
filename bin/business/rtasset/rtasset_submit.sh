#!/usr/bin/env bash

# /usr/hdp/2.2.0.0-2041/spark24/bin/spark-submit \
#  --class "leongu.myspark._business.rt_asset.RTAsset" \
#  --master yarn \
#  --deploy-mode cluster \
#  --driver-memory 1g \
#  --executor-memory 2g \
#  --executor-cores 1 \
#  --num-executors 4 \
#  --files /data/gulele/spark/rtasset/rtasset.yaml \
#  --queue root \
#  /data/gulele/spark/myspark-1.0.0-jar-with-dependencies.jar \
#  rtasset.yaml

spark-submit \
 --class "leongu.myspark._business.rt_asset.RTAsset" \
 --master yarn \
 --deploy-mode cluster \
 --driver-memory 1g \
 --executor-memory 1g \
 --executor-cores 1 \
 --files /Users/apple/workspaces/github/_my/myspark/bin/business/rtasset/mac.yaml \
 --queue root \
 /Users/apple/workspaces/github/_my/myspark/target/myspark-1.0.0-jar-with-dependencies.jar \
 mac.yaml