### Docker images for MindsDB

* https://docs.mindsdb.com/deployment/docker/
* https://hub.docker.com/u/mindsdb

## Building

Build latest MindsDB from https://pypi.org/project/MindsDB/

    docker build -f release -t mindsdb/mindsdb .

Build specific MindsDB version.

    docker build -f release --build-arg VERSION=2.57.0 -t mindsdb/mindsdb .

### `beta` vs `release`

`release` image pins MindsDB version. `beta` updates MindsDB when container
is started if a new version is available.

## Releasing

The `build.py <beta|release>` script is used in CI to build and push images
on release.
