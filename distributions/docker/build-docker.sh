#!/bin/bash
#
# This creates a Docker image of the current local checkout of the MindsDB
# Project.
#
# If you like once you make this docker image you can push it somewhere
# (eg: dockerhub)
#

# Farley's super crazy helper which ensures you are in the folder of the repo no matter where this script is ran from
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
cd $DIR/../..

cp distributions/docker/Dockerfile ./
cp distributions/docker/requirements-docker.txt ./

docker build .

rm -f ./Dockerfile requirements-docker.txt > /dev/null 2>&1
