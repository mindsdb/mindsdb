# This is specified as an ARG so that we can set it to an nvidia base image for gpu-compatible ml engines
ARG BASE_IMAGE=python:3.10-slim

FROM ${BASE_IMAGE} as build

# List of pip packages to install on top of bare mindsdb
ARG EXTRAS
WORKDIR /mindsdb

# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y python3 python3-pip git freetds-dev  # freetds required to build pymssql for mssql_handler


# Copy just requirements and install them to cache the layer. This won't include any of the default handlers,
# but this layer should rarely change so will be cached most of the time
COPY requirements/requirements.txt /mindsdb/requirements/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip3 install --user -r requirements/requirements.txt


# Now copy the rest of the code and install it
COPY . /mindsdb
RUN --mount=type=cache,target=/root/.cache/pip pip3 install --user "."


# Install extras on top of the bare mindsdb
RUN --mount=type=cache,target=/root/.cache/pip if [ -n "$EXTRAS" ]; then pip3 install --user $EXTRAS; fi


# For use in docker-compose/development. Installs our development requirements
FROM build as dev

# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y libmagic1 libpq5 freetds-bin
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements/requirements-dev.txt

COPY docker/mindsdb_config.release.json /root/mindsdb_config.json

# Makes sure we see all output in the container logs
ENV PYTHONUNBUFFERED=1

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "sh", "-c", "python3 -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]


# Copy installed pip packages and install only what we need
FROM ${BASE_IMAGE}
# FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04
# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y python3 python3-pip libmagic1 libpq5 freetds-bin

# Copy python packages from the build stage
COPY --link --from=build /root/.local/lib/python3.10 /root/.local/lib/python3.10
COPY docker/mindsdb_config.release.json /root/mindsdb_config.json

# Makes sure we see all output in the container logs
ENV PYTHONUNBUFFERED=1

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "sh", "-c", "python3 -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]
