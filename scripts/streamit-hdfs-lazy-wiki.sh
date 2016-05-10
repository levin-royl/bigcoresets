#!/bin/bash

echo reading input file $1

hadoop fs -rmr hdfs:///user/royl/tmp
hadoop fs -mkdir hdfs:///user/royl/tmp
hadoop fs -rmr hdfs:///user/royl/data/streamin
hadoop fs -mkdir hdfs:///user/royl/data/streamin

sleepTime=10
numLines=$[500*8*10*10]
totalLines=`hadoop fs -cat $1 | wc -l`

read -p "Press any key to continue... " -n1 -s
echo " OK"
i=0

while [ $i -lt $totalLines ]
do
	h=$[$numLines+$i]
	echo processing $numLines out of $totalLines lines from $i

	hadoop fs -cat $1 | head -$h | tail -$totalLines > streamin/part-${i}.txt
        hadoop fs -cp streamin/part-${i}.txt hdfs:///user/royl/tmp/.
        hadoop fs -mv hdfs:///user/royl/tmp/* hdfs:///user/royl/data/streamin/.
	rm streamin/part-${i}.txt

	i=$[$i+$numLines]
	echo sleeping ...
	sleep sleepTime
done
