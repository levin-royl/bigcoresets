#!/bin/bash

#split -a5 -d -l 40000 wiki_vecs.txt wiki_vecs/wiki_vec

sleep_time=$1
echo sleep time = $sleep_time

hadoop fs -rmr hdfs:///user/spark/tmp
hadoop fs -mkdir hdfs:///user/spark/tmp
hadoop fs -rmr hdfs:///user/spark/streamin
hadoop fs -mkdir hdfs:///user/spark/streamin

files=`hadoop fs -ls s3://aws-logs-773707194450-eu-west-1/elasticmapreduce/wiki_vecs_some | grep wiki_vecs  | cut -d" " -f8 | sort`

read -p "Press any key to continue... " -n1 -s
echo " OK"
i=0
j=0

for f in ${files[@]}; do 
	before=`date +%s%3N`

	echo "[$j][$i] Copying $f"; 

        hadoop fs -cp $f hdfs:///user/spark/tmp/.
        hadoop fs -mv hdfs:///user/spark/tmp/* hdfs:///user/spark/streamin/.

	i=$((i + 1))
	j=$((j + 1))

	after=`date +%s%3N`
	delta=$(( after - before ))

	echo took $delta milliseconds

	if [ "$i" -ge "1" ]; then
		echo "sleeping for $sleep_time seconds ..."
		sleep $sleep_time
		i=0
	fi
done
