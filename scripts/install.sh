#!/bin/bash

#scp -i ~/coreset_spark_ireland.pem /cygdrive/c/Users/Roy/git/bigcoresets/scripts/*.sh $AMAZON:///home/hadoop/.

wget http://apache.mivzakim.net/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar xzvf apache-maven-3.3.9-bin.tar.gz
sudo mv apache-maven-3.3.9-bin opt/.
echo "" >> ~/.bashrc
echo 'export PATH=/opt/apache-maven-3.3.9/bin:$PATH' >> ~/.bashrc
sudo yum -y install git

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
