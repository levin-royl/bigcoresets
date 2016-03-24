#!/bin/bash

#		-i socket://ir-cluster02.haifa.ibm.com:9999

spark-submit --class streaming.coresets.App \
	--master yarn \
	--deploy-mode client \
	--driver-memory 4G \
	--executor-memory 4G \
	--executor-cores 1 \
	--num-executors 10 \
	--conf spark.driver.maxResultSize=20g \
	target/bigcoresets-1.0.jar \
		--checkpointDir hdfs:///user/royl/checkpoint \
		--denseData \
		-v \
		-i hdfs:///user/royl/data/streamin \
		-o hdfs:///user/royl/data/out/streaming-coreset-kmeans/artho.vec \
		-a coreset-kmeans \
		--algorithmParams 100 \
		--sampleSize 1024 \
		--batchSecs 10 \
		--parallelism 10 \
		-m streaming
