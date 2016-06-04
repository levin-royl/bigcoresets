#!/bin/bash

outName=$1

hadoop fs -rmr hdfs:///user/spark/checkpoint
hadoop fs -mkdir hdfs:///user/spark/checkpoint

#		-i socket://ir-cluster02.haifa.ibm.com:9999
#		-i hdfs:///user/spark/wiki_vecs.txt \
	
spark-submit --class streaming.coresets.App \
	--master yarn \
	--deploy-mode client \
	--driver-memory 8G \
	--executor-memory 8G \
	--executor-cores 8 \
	--num-executors 10 \
	--conf spark.driver.maxResultSize=20g \
	proj/bigcoresets/target/bigcoresets-1.0.jar \
		--checkpointDir hdfs:///user/spark/checkpoint \
		-v \
		-i s3://aws-logs-773707194450-eu-west-1/elasticmapreduce/wiki_vecs.txt \
		-o hdfs:///user/spark/$outName \
		-a coreset-kmeans \
		--dim 100000 \
		--algorithmParams 10 \
		--parallelism 80000 \
		-m evaluate
