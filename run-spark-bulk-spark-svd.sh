#!/bin/bash

#		-i socket://ir-cluster02.haifa.ibm.com:9999 \

spark-submit --class streaming.coresets.App \
	--master yarn \
	--deploy-mode client \
	--queue Social_Analytics \
	--driver-memory 4G \
	--executor-memory 16G \
	--executor-cores 1 \
	--num-executors 11 \
	--conf spark.shuffle.blockTransferService=nio \
	--conf spark.driver.maxResultSize=20g \
	--conf spark.streaming.receiver.maxRate=32768 \
	--conf spark.streaming.receiver.writeAheadLog.enable=true \
	bigcoresets-1.0.jar \
		--checkpointDir hdfs:///user/royl/checkpoint \
		--denseData \
		-v \
		-i hdfs:///user/royl/data/streamin \
		-o hdfs:///user/royl/data/wiki-out/streaming-coreset-kmeans/artho.vec \
		-a coreset-kmeans \
		--algorithmParams 100 \
		--sampleSize 1024 \
		--batchSecs 10 \
		--parallelism 11 \
		-m streaming
