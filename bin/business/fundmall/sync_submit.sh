#!/usr/bin/env bash

export hadoop_security_authentication_sdp_publickey=l8azpZthONruJqs8IzCS7oeMXyzXA2HhON69;
export hadoop_security_authentication_sdp_privatekey=DQyW09NjTRrkTBu5yuceaSqWEn0VL3ST;
export hadoop_security_authentication_sdp_username=yarn;

cd fund_mall_new_daily

/usr/hdp/2.2.0.0-2041/spark24/bin/spark-submit \
 --conf "spark.driver.extraLibraryPath=/usr/hdp/2.2.0.0-2041/hadoop/lib/native/" \
 --class "leongu.myspark._business.fundmall.Fundmall" \
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