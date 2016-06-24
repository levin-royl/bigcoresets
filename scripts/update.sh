#!/bin/bash

cd proj/Java-KMeans-Coreset
git pull origin
mvn install

cd ../bigcoresets
git pull origin
mvn package

cd ../..
