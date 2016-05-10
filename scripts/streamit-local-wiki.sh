#!/bin/bash

#hadoop fs -rmr /user/royl/data/streamin
#hadoop fs -mkdir /user/royl/data/streamin

rm -rf wiki-out
mkdir wiki-out

rm -rf streamin
mkdir streamin

read -p "Press any key to continue... " -n1 -s
echo " OK"
i=0
j=0

for f in wiki_vecs/*; do 
	echo "[$j][$i] Copying $f"; 
#	hadoop fs -put $f /user/royl/data/streamin/.
	cp $f streamin/.

	i=$((i + 1))
	j=$((j + 1))

	if [ "$i" -eq "80" ]; then
		echo "sleeping ..."
		sleep 1500
		i=0
	fi
done
