#!/bin/bash

./streamit-hdfs-wiki.sh \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/wiki_vecs \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streamin \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/tmp \
	wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streaming-uniform-coreset-kmeans-out
