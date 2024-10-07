# syntax=docker/dockerfile:1.7-labs
# Syntax declaration is for "--exclude" below

FROM python:3.10 as build
WORKDIR /mindsdb
ARG EXTRAS

# Copy everything to get requirements files.
# Exclude as many other types of files as possible to avoid invalidating the cache
COPY    --exclude=**/*.py \
        --exclude=**/*.md \
        --exclude=**/*.yml \
        --exclude=**/*.html \
        --exclude=**/*.pdf \
        --exclude=**/*.csv \
        --exclude=**/*.svg \
        --exclude=**/*.ini \
        --exclude=**/*.js \
        --exclude=**/*.json \
        --exclude=**/*.css \
        --exclude=**/*.Dockerfile \
        --exclude=**/*.toml \
        --exclude=**/*.pem \
        --exclude=**/*.png \
        --exclude=**/*.db \
        --exclude=**/*.ico \
        . .

# Copy setup.py and everything else used by setup.py
COPY setup.py default_handlers.txt README.md pyproject.toml ./
COPY mindsdb/__about__.py mindsdb/

# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update -qy \
    && apt-get upgrade -qy \
    && apt-get install -qy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    freetds-dev  # freetds required to build pymssql on arm64 for mssql_handler. Can be removed when we are on python3.11+

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# - Silence uv complaining about not being able to use hard links,
# - tell uv to byte-compile packages for faster application startups,
# - prevent uv from accidentally downloading isolated Python builds,
# - pick a Python,
# - and finally declare `/app` as the target for `uv sync`.
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.10 \
    UV_PROJECT_ENVIRONMENT=/mindsdb

# These long-running step should be cached in most cases (unless a requirements file or setup.py etc is changed)
# Install all requirements for mindsdb and all the default handlers
RUN --mount=type=cache,target=/root/.cache uv venv
RUN --mount=type=cache,target=/root/.cache uv pip install "."
# Install extras on top of the bare mindsdb
RUN --mount=type=cache,target=/root/.cache if [ -n "$EXTRAS" ]; then uv pip install $EXTRAS; fi

# Copy all of the mindsdb code over finally
COPY . .
# Install the "mindsdb" package now that we have the code for it
RUN --mount=type=cache,target=/root/.cache uv pip install --no-deps "."




# Same as build image, but with dev dependencies installed.
# This image is used in our docker-compose
FROM build as dev
WORKDIR /mindsdb

# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update -qy \
    && apt-get upgrade -qy \
    && apt-get install -qy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    libpq5 freetds-bin curl
RUN --mount=type=cache,target=/root/.cache uv pip install -r requirements/requirements-dev.txt

COPY docker/mindsdb_config.release.json /root/mindsdb_config.json


ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1
ENV PATH=/mindsdb/.venv/bin:$PATH

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "bash", "-c", "python -Im mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]




# This is the final image for most use-cases
# Copies the installed pip packages from `build` and installs only what we need
FROM python:3.10-slim
WORKDIR /mindsdb

# "rm ... docker-clean" stops docker from removing packages from our cache
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update -qy \
    && apt-get upgrade -qy \
    && apt-get install -qy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    libpq5 freetds-bin curl direnv

RUN echo "source .venv/bin/activate" >> .envrc && echo 'eval "$(direnv hook bash)"' >> .envrc

COPY --link --from=build /mindsdb /mindsdb
COPY docker/mindsdb_config.release.json /root/mindsdb_config.json

ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1
ENV PATH=/mindsdb/.venv/bin:$PATH

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "sh", "-c", "python -Im mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]
