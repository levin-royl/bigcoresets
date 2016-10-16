#!/bin/bash

./run-spark.sh \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streamin \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streaming-coreset2-kmeans-out \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/checkpoint \
	coreset2-kmeans
