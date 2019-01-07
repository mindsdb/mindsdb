[<Back to Table of Contents](../../README.md)

# Build your docker container

Assuming that you have [docker](https://docs.docker.com/install/) installed in your computer.
on your terminal, you can do the following:

```
mkdir mindsdb_docker
cd mindsdb_docker
curl https://raw.githubusercontent.com/mindsdb/mindsdb/master/distributions/docker/Dockerfile > Dockerfile
curl https://raw.githubusercontent.com/mindsdb/mindsdb/master/distributions/docker/build-docker.sh > build-docker.sh
curl https://raw.githubusercontent.com/mindsdb/mindsdb/master/distributions/docker/requirements-docker.txt > requirements-docker.txt
sh build-docker.sh
```
