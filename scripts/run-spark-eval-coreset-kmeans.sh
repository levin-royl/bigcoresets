#!/bin/bash

#		-i socket://ir-cluster02.haifa.ibm.com:9999

spark-submit --class streaming.coresets.App \
	--master yarn \
	--deploy-mode client \
	--driver-memory 2G \
	--executor-memory 2G \
	--executor-cores 2 \
	--num-executors 10 \
	--conf spark.driver.maxResultSize=20g \
	target/bigcoresets-1.0.jar \
		--checkpointDir hdfs:///user/royl/checkpoint \
		--denseData \
		-v \
		-i hdfs:///user/royl/data/big.txt \
		-o hdfs:///user/royl/data/out/streaming-coreset-kmeans \
		-a coreset-kmeans \
		--algorithmParams 100 \
		--parallelism 10 \
		-m evaluate
