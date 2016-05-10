#!/bin/bash

echo reading input file $1

rm -rf streamin
mkdir streamin

sleepTime=600
numLines=$[500*8*10*10]
totalLines=`wc -l $1`

read -p "Press any key to continue... " -n1 -s
echo " OK"
i=0

while [ $i -lt $totalLines ]
do
	h=$[$numLines+$i]
	echo processing $numLines out of $totalLines lines from $i
	cat $1 | head -$h | tail -$totalLines > streamin/part-${i}.txt
	i=$[$i+$numLines]
	echo sleeping ...
	sleep sleepTime
done
