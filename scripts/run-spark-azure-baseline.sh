#!/bin/bash

./run-spark.sh \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streamin \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streaming-uniform-coreset-kmeans-out \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/checkpoint \
	coreset-uniform-kmeans
