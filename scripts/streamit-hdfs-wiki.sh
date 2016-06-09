#!/bin/bash

#split -a5 -d -l 40000 wiki_vecs.txt wiki_vecs/wiki_vec

path=$1

hadoop fs -rmr hdfs:///user/spark/tmp
hadoop fs -mkdir hdfs:///user/spark/tmp
hadoop fs -rmr hdfs:///user/spark/streamin
hadoop fs -mkdir hdfs:///user/spark/streamin

files=`hadoop fs -ls s3://aws-logs-773707194450-eu-west-1/elasticmapreduce/wiki_vecs_some | grep wiki_vecs  | cut -d" " -f8 | sort`

read -p "Press any key to continue... " -n1 -s
echo " OK"
i=0

for f in ${files[@]}; do 
	before=`date +%s%3N`

	echo "[$i] Copying $f"; 

        hadoop fs -cp $f hdfs:///user/spark/tmp/.
        hadoop fs -mv hdfs:///user/spark/tmp/* hdfs:///user/spark/streamin/.

	i=$((i + 1))

	old_cnt=`hadoop fs -ls $path | wc -l`
	chg=$((new_cnt - old_cnt))

	echo old_cnt=$old_cnt

	while [ $chg -eq 0 ]
	do
		new_cnt=`hadoop fs -ls $path | wc -l`
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
