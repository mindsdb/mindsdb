### Docker images for MindsDB

* https://docs.mindsdb.com/setup/self-hosted/docker/
* https://hub.docker.com/u/mindsdb

## Building

Docker images are using only released versions of MindsDB from
https://pypi.org/project/MindsDB/ so no files in parent dir are used.

To build `release` image using version reported at
https://public.api.mindsdb.com/installer/release/docker___success___None

    docker build -f release --no-cache -t mindsdb/mindsdb

To build `release` image with specific MindsDB version.

    docker build -f release --build-arg VERSION=2.57.0 -t mindsdb/mindsdb

### `beta` vs `release`

`release` image pins MindsDB version and builds from fixed PyTorch docker
image. `beta` uses latest PyTorch image and updates MindsDB when container
is started to a version set at
https://public.api.mindsdb.com/installer/beta/docker___success___None

## Releasing

The `build.py <beta|release>` script is used in CI to build and push images
on release.

## Running local docker compose environment (in development)

Run `docker-compose up` or `docker-compose up -d` (for `detach` mode) to launch mindsdb environment in docker compose


## Running local docker compose environment (in old manner development)



1. Run `docker-compose -f docker-compose-old-manner up` or `docker-compose -f docker-compose-old-manner up -d` (for `detach` mode) to launch mindsdb in docker-compose in old school manner (monolithic on 100%)
