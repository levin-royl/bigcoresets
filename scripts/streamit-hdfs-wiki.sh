#!/bin/bash

#split -a5 -d -l 40000 wiki_vecs.txt wiki_vecs/wiki_vec

hadoop fs -rmr hdfs:///user/royl/tmp
hadoop fs -mkdir hdfs:///user/royl/tmp
hadoop fs -rmr hdfs:///user/royl/data/streamin
hadoop fs -mkdir hdfs:///user/royl/data/streamin

#rm -rf ~/data/coresets/streamin
#mkdir ~/data/coresets/streamin

files=`hadoop fs -ls hdfs:///user/royl/data/wiki_vecs | cut -d" " -f11`

read -p "Press any key to continue... " -n1 -s
echo " OK"
i=0
j=0

for f in ${files[@]}; do 
#for f in wiki_vecs/*; do 
	echo "[$j][$i] Copying $f"; 

        hadoop fs -cp $f hdfs:///user/royl/tmp/.
        hadoop fs -mv hdfs:///user/royl/tmp/* hdfs:///user/royl/data/streamin/.

#	cp $f ~/data/coresets/streamin/.

	i=$((i + 1))
	j=$((j + 1))

	if [ "$i" -ge "1" ]; then
		echo "sleeping ..."
		sleep 100
		i=0
	fi
done
