# Bare mindsdb with no extras is built as a separate stage for caching
FROM python:3.10 as build
# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y freetds-dev  # freetds required to build pymssql for mssql_handler

WORKDIR /mindsdb

# Copy just requirements and install them to cache the layer
# This won't include any of the default handlers, but it should still speed things up
COPY requirements/requirements.txt /mindsdb/requirements/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements/requirements.txt


# Now copy the rest of the code and install it
COPY . /mindsdb
RUN --mount=type=cache,target=/root/.cache/pip pip install "."


# Install extras on top of the bare mindsdb
FROM build as extras
ARG EXTRAS
RUN --mount=type=cache,target=/root/.cache/pip if [ -n "$EXTRAS" ]; then pip install $EXTRAS; fi


# For use in docker-compose
FROM extras as dev
# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y libmagic1 libpq5 freetds-bin
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements/requirements-dev.txt

COPY docker/mindsdb_config.release.json /root/mindsdb_config.json


ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "sh", "-c", "python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]


# Copy installed pip packages and install only what we need
FROM python:3.10-slim
# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y libmagic1 libpq5 freetds-bin

COPY --link --from=extras /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY docker/mindsdb_config.release.json /root/mindsdb_config.json

ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "sh", "-c", "python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]
