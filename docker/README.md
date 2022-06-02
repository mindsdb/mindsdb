### Docker images for MindsDB

* https://docs.mindsdb.com/deployment/docker/
* https://hub.docker.com/u/mindsdb

`release` image comes with pinned version. `beta` tries to update the
shipped version when run. MindsDB is fetched from https://pypi.org

Use `build.py <beta|release` to get commands for building and publishing
the images for the latest release.

Or to build an image for a fixed MindsDB version.

    docker build -f release --build-arg VERSION=2.57.0 -t mindsdb/mindsdb
