# This first stage produces a file structure with ONLY files required to run `pip install .`
# We want to do this because we have a lot of dependencies which take a long time to install
# So we avoid invalidating the cache of `pip install .` at all costs by ignoring every other file
FROM python:3.10 as deps
WORKDIR /mindsdb

# Copy everything to begin with
COPY . .
# Find every FILE that is not a requirements file and delete it
RUN find ./ -type f -not -name "requirements*.txt" -print | xargs rm -f
# Find every empty directory and delete them
RUN find ./ -type d -empty -delete
# Copy setup.py and everything else used by setup.py
COPY setup.py default_handlers.txt README.md ./
COPY mindsdb/__about__.py mindsdb/




# This stage copies the structure left from the previous stage and runs `pip install .`
# This long-running step should be cached in most cases (unless a requirements file or setup.py etc is changed)
FROM python:3.10 as build
WORKDIR /mindsdb
ARG EXTRAS

# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y freetds-dev  # freetds required to build pymssql on arm64 for mssql_handler. Can be removed when we are on python3.11+

# Copy the requirements files, setup.py etc from above
COPY --from=deps /mindsdb .
# Install all requirements for mindsdb and all the default handlers
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked pip install "."
# Install extras on top of the bare mindsdb
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked if [ -n "$EXTRAS" ]; then pip install $EXTRAS; fi

# Copy all of the mindsdb code over finally
COPY . .
# Install the "mindsdb" package now that we have the code for it
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked pip install "."




# Same as build image, but with dev dependencies installed.
# This image is used in our docker-compose
FROM build as dev
WORKDIR /mindsdb

# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y libpq5 freetds-bin curl
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked pip install -r requirements/requirements-dev.txt

COPY docker/mindsdb_config.release.json /root/mindsdb_config.json


ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "sh", "-c", "python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]




# This is the final image for most use-cases
# Copies the installed pip packages from `build` and installs only what we need
FROM python:3.10-slim
WORKDIR /mindsdb

# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y \
    && apt-get install -y libpq5 freetds-bin curl

COPY --link --from=build /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY docker/mindsdb_config.release.json /root/mindsdb_config.json
COPY . .

ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "sh", "-c", "python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]