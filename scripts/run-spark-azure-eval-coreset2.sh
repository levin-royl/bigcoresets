#!/bin/bash

./run-spark-eval.sh \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/wiki_vecs \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streaming-coreset-kmeans-out \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/checkpoint \
	coreset-kmeans
