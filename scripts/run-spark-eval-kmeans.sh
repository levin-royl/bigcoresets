#!/bin/bash

#		-i socket://ir-cluster02.haifa.ibm.com:9999

spark-submit --class streaming.coresets.App \
	--master yarn \
	--deploy-mode client \
	--driver-memory 8G \
	--executor-memory 8G \
	--executor-cores 8 \
	--num-executors 10 \
	--conf spark.driver.maxResultSize=20g \
	proj/bigcoresets/target/bigcoresets-1.0.jar \
		--checkpointDir hdfs:///user/royl/checkpoint \
		-v \
		-i hdfs:///user/spark/wiki_vecs.txt \
		-o hdfs:///user/spark/streaming-coreset-kmeans-out \
		-a coreset-kmeans \
		--dim 100000 \
		--algorithmParams 10 \
		--parallelism 80000 \
		-m evaluate
