#!/bin/bash

#./run-spark.sh wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streamin wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streaming-coreset-kmeans-out wasb://contmsspark@samsspark.blob.core.windows.net/coreset/checkpoint

fromPath=$1
toPath=$2
checkpointPath=$3

toPath=$2
tmpPath=$3
listenPath=$4

hadoop fs -rmr $checkpointPath
hadoop fs -mkdir $checkpointPath

hadoop fs -rmr $toPath
hadoop fs -mkdir $toPath

spark-submit \
	--class streaming.coresets.App \
	--master yarn \
	--deploy-mode client \
	--driver-memory 10G \
	--executor-memory 10G \
	--executor-cores 4 \
	--num-executors 13 \
	--conf spark.driver.maxResultSize=20g \
	~/coreset/proj/bigcoresets/target/bigcoresets-1.0.jar \
		--checkpointDir $$checkpointPath \
		-v \
		-i $fromPath \
		-o $toPath/artho.vec \
		-a coreset-kmeans \
		--dim 100000 \
		--algorithmParams 10 \
		--sampleSize 256 \
		--batchSecs 300 \
		--parallelism 520 \
		-m streaming
