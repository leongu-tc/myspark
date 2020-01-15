#!/usr/bin/env bash

export hadoop_security_authentication_sdp_publickey=l8azpZthONruJqs8IzCS7oeMXyzXA2HhON69;
export hadoop_security_authentication_sdp_privatekey=DQyW09NjTRrkTBu5yuceaSqWEn0VL3ST;
export hadoop_security_authentication_sdp_username=yarn;

/usr/hdp/2.2.0.0-2041/spark24/bin/spark-submit \
 --class "leongu.myspark._business.assetanalysis.AssetAnalysis" \
 --master yarn \
 --deploy-mode cluster \
 --driver-memory 1g \
 --executor-memory 2g \
 --executor-cores 1 \
 --num-executors 4 \
 --files sync.yaml \
 --queue root \
 /data/gulele/spark/myspark-1.0.0-jar-with-dependencies.jar \
 sync.yaml
