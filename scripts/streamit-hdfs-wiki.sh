#!/bin/bash

# 512 lines X 52 cores X 10 for each core
#split -a5 -d -l 266240 wiki_vecs.txt wiki_vecs/wiki_vec
#./streamit-hdfs-wiki.sh wasb://contmsspark@samsspark.blob.core.windows.net/coreset/wiki_vecs wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streamin wasb://contmsspark@samsspark.blob.core.windows.net/coreset/tmp wasb://contmsspark@samsspark.blob.core.windows.net/coreset/streaming-coreset-kmeans-out

fromPath=$1
toPath=$2
tmpPath=$3
listenPath=$4

hadoop fs -rmr $tmpPath
hadoop fs -mkdir $tmpPath
hadoop fs -rmr $toPath
hadoop fs -mkdir $toPath

#files=`hadoop fs -ls s3://aws-logs-773707194450-eu-west-1/elasticmapreduce/wiki_vecs_some | grep wiki_vecs  | cut -d" " -f8 | sort`
files=`hadoop fs -ls $fromPath | grep wiki_vecs | tr -s ' ' | tr ' ' '=' | cut -d= -f8 | sort`

read -p "Press any key to continue... " -n1 -s
echo " OK"
i=0

old_cnt=0

for f in ${files[@]}; do 
	before=`date +%s%3N`

	echo "[$i] Copying $f"; 

	hadoop fs -cp $f $tmpPath
	hadoop fs -mv $tmpPath/* $toPath/.

	i=$((i + 1))

	new_cnt=`hadoop fs -ls $listenPath | wc -l`
	chg=$((new_cnt - old_cnt))

	echo old_cnt=$old_cnt

	while [ $chg -eq 0 ]
	do
		new_cnt=`hadoop fs -ls $listenPath | wc -l`
		chg=$((new_cnt - old_cnt))
		sleep 5
	done

	old_cnt=$new_cnt

	echo new_cnt=$new_cnt
	echo chg=$chg

	after=`date +%s%3N`
	delta=$(( after - before ))

	echo took $delta milliseconds
done
