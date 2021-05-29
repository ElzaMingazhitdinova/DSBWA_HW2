#!/bin/bash

if [[ $# -eq 0 ]] ; then
    echo 'You should specify container name!'
    exit 1
fi

docker pull zoltannz/hadoop-ubuntu:2.8.1
docker stop $(docker ps | grep $1 | awk '{print $1}')
docker rm $(docker ps -a | grep $1 | awk '{print $1}')

# docker build -t myhadoopspark .

docker run -d --name $1 \
            -p 2122:2122 -p 8020:8020 -p 8030:8030 -p 8040:8040 -p 8042:8042 -p 8088:8088 -p 9000:9000 -p 10020:10020 -p 19888:19888 -p 49707:49707 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 -p 50090:50090 \
            -t zoltannz/hadoop-ubuntu:2.8.1

echo "Creating .jar file..."
mvn package -f ../pom.xml

docker cp ../target/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar $1:/tmp
docker cp start.sh $1:/

docker exec $1 bash start.sh database_name

mkdir research_result
docker cp $1:/execution_time_dataIntensive.log research_result/execution_time_dataIntensive.log
docker cp $1:/execution_time_computeIntensive.log research_result/execution_time_computeIntensive.log
