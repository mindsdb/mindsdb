#!/bin/bash
#
# This creates a Docker image of the current local checkout of the MindsDB
# Project.
#
# If you like once you make this docker image you can push it somewhere
# (eg: dockerhub)
#

mkdir mindsdb_docker
cd mindsdb_docker
curl https://raw.githubusercontent.com/mindsdb/mindsdb/stable/distributions/docker/Dockerfile > Dockerfile
docker build -t mindsdb .
cd ..
rm -rf mindsdb_docker > /dev/null 2>&1

echo "Do you want to run MindsDB Server container (yes/no)?"
read run

if [ "$run" = "yes" ]; then
    echo "Running Mindsdb Server container..."
    docker run -d -it --name=mindsdb mindsdb
    docker exec -it mindsdb python
fi
