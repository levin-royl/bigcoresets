#!/bin/bash

#		-i socket://ir-cluster02.haifa.ibm.com:9999

hadoop fs -rmr hdfs:///user/spark/checkpoint
hadoop fs -mkdir hdfs:///user/spark/checkpoint

hadoop fs -rmr hdfs:///user/spark/streaming-coreset-uniform-kmeans-out
hadoop fs -mkdir hdfs:///user/spark/streaming-coreset-uniform-kmeans-out

spark-submit \
	--class streaming.coresets.App \
	--master yarn \
	--deploy-mode client \
	--driver-memory 10G \
	--executor-memory 10G \
	--executor-cores 8 \
	--num-executors 10 \
	--conf spark.driver.maxResultSize=20g \
	proj/bigcoresets/target/bigcoresets-1.0.jar \
		--checkpointDir hdfs:///user/spark/checkpoint \
		-v \
		-i hdfs:///user/spark/streamin \
		-o hdfs:///user/spark/streaming-coreset-uniform-kmeans-out/artho.vec \
		-a coreset-uniform-kmeans \
		--dim 100000 \
		--algorithmParams 10 \
		--sampleSize 256 \
		--batchSecs 400 \
		--parallelism 800 \
		-m streaming
