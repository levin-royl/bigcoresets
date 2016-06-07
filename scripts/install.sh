#!/bin/bash

#scp -i ~/coreset_spark_ireland.pem /cygdrive/c/Users/Roy/git/bigcoresets/scripts/*.sh $AMAZON:///home/hadoop/.

myconf=`cat ~/.bashrc`

if [[ $myconf != *"maven"* ]]
then
	echo Installing Maven
	rm -rf apache-maven-3.3.9-bin.tar.gz
	rm -rf apache-maven-3.3.9
	wget http://apache.mivzakim.net/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
	tar xzvf apache-maven-3.3.9-bin.tar.gz
	rm -rf apache-maven-3.3.9-bin.tar.gz
	sudo rm -rf /opt/apache-maven-3.3.9
	sudo mv apache-maven-3.3.9 /opt/.
	echo "" >> ~/.bashrc
	echo 'export PATH=/opt/apache-maven-3.3.9/bin:$PATH' >> ~/.bashrc
	source ~/.bashrc
fi

sudo yum -y install git

rm -rf proj
mkdir proj
cd proj

git clone https://github.com/C0rWin/Java-KMeans-Coreset.git
cd Java-KMeans-Coreset
mvn clean install
cd ..

git clone https://github.com/levin-royl/bigcoresets.git
cd bigcoresets
mvn clean package
cd ..

cd ..

#git pull origin

