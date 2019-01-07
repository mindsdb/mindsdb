[<Back to Table of Contents](../../README.md)

# Build your docker container

Assuming that you have [docker](https://docs.docker.com/install/) installed in your computer.
on your terminal, you can do the following:

```
mkdir mindsdb_docker
cd mindsdb_docker
curl https://raw.githubusercontent.com/mindsdb/mindsdb/master/distributions/docker/Dockerfile > Dockerfile
docker build -t mindsdb .
docker run -d -it  --name=mindsdb mindsdb
docker exec -it mindsdb python
echo "MindsDB docker done."
```
