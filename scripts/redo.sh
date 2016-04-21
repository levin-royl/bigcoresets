#!/bin/bash

rm -rf latest
cp -r ~/git/bigcoresets latest
cd latest
mvn clean install -DskipTests
mvn package -DskipTests
cd ..
