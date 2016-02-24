#!/bin/bash

spark-submit --class streaming.coresets \
    --master client \
    --deploy-mode cluster \
    --queue Social_Analytics \
    --driver-memory 4G \
    --executor-memory 200G \
    --executor-cores 4 \
    --num-executors 11 \
    --conf spark.shuffle.blockTransferService=nio \
    WCS_RnR-0.0.1-SNAPSHOT.jar \
        --checkpointDir hdfs:///user/royl/checkpoint \
        -v
        -i hdfs:///user/royl/data/wiki_vecs.txt
        -o hdfs:///user/royl/data/wiki-out/spark-svd/artho.vec
        -a spark-svd
        --batchSecs 10
        --parallelism 44
        -m bulk
