# This stage's objective is to gather ONLY requirements.txt files and anything else needed to install deps.
# This stage will be run almost every build, but it is fast and the resulting layer hash will be the same unless a deps file changes.
# We do it this way because we can't copy all requirements files with a glob pattern in docker while maintaining the folder structure.
FROM python:3.10 as deps
WORKDIR /mindsdb
ARG EXTRAS

# Copy everything to begin with
# This will almost always invalidate the cache for this stage
COPY . .
# Find every FILE that is not a requirements file and delete it
RUN find ./ -type f -not -name "requirements*.txt" -print | xargs rm -f \
# Find every empty directory and delete it
    && find ./ -type d -empty -delete
# Copy setup.py and everything else used by setup.py
COPY setup.py default_handlers.txt README.md ./
COPY mindsdb/__about__.py mindsdb/
# Now this stage only contains a few files and the layer hash will be the same if they don't change.
# Which will mean the next stage can be cached, even if the cache for the above stage was invalidated.



# Use the stage from above to install our deps with as much caching as possible
FROM python:3.10 as build
WORKDIR /mindsdb
ARG EXTRAS

# Configure apt to retain downloaded packages so we can store them in a cache mount
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
# Install system dependencies, with caching for faster builds
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update -qy \
    && apt-get upgrade -qy \
    && apt-get install -qy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    freetds-dev  # freetds required to build pymssql on arm64 for mssql_handler. Can be removed when we are on python3.11+

# Use a specific tag so the file doesn't change
COPY --from=ghcr.io/astral-sh/uv:0.4.23 /uv /usr/local/bin/uv
# Copy requirements files from the first stage
COPY --from=deps /mindsdb .

# - Silence uv complaining about not being able to use hard links,
# - tell uv to byte-compile packages for faster application startups,
# - prevent uv from accidentally downloading isolated Python builds,
# - pick a Python,
# - and finally declare `/mindsdb` as the target dir.
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.10 \
    UV_PROJECT_ENVIRONMENT=/mindsdb \
    VIRTUAL_ENV=/venv \
    PATH=/venv/bin:$PATH

# Install all requirements for mindsdb and all the default handlers
# Installs everything into a venv in /mindsdb so that everything is isolated
RUN --mount=type=cache,target=/root/.cache \
    uv venv /venv \
    && uv pip install pip "."
# Install extras on top of the bare mindsdb
RUN --mount=type=cache,target=/root/.cache \
    if [ -n "$EXTRAS" ]; then uv pip install $EXTRAS; fi

# Copy all of the mindsdb code over finally
# Here is where we invalidate the cache again if ANY file has changed
COPY . .
# Install the "mindsdb" package now that we have the code for it
RUN --mount=type=cache,target=/root/.cache uv pip install --no-deps "."




# Same as build image, but with dev dependencies installed.
# This image is used in our docker-compose
FROM build as dev
WORKDIR /mindsdb

# Configure apt to retain downloaded packages so we can store them in a cache mount
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
# Install system dependencies, with caching for faster builds
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update -qy \
    && apt-get upgrade -qy \
    && apt-get install -qy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    libpq5 freetds-bin curl

# Install dev requirements and install 'mindsdb' as an editable package
RUN --mount=type=cache,target=/root/.cache uv pip install -r requirements/requirements-dev.txt \
                                        && uv pip install --no-deps -e "."

COPY docker/mindsdb_config.release.json /root/mindsdb_config.json


ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1
ENV VIRTUAL_ENV /venv
ENV PATH /venv/bin:$PATH

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "bash", "-c", "python -Im mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]




# This is the final image for most use-cases
# Copies the installed pip packages from `build` and installs only what we need to run
FROM python:3.10-slim
WORKDIR /mindsdb

# Configure apt to retain downloaded packages so we can store them in a cache mount
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
# Install system dependencies, with caching for faster builds
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update -qy \
    && apt-get upgrade -qy \
    && apt-get install -qy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    libpq5 freetds-bin curl

# Copy installed packages and venv from the build stage
COPY --link --from=build /venv /venv
COPY --link --from=build /mindsdb /mindsdb
COPY docker/mindsdb_config.release.json /root/mindsdb_config.json

ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1
ENV VIRTUAL_ENV /venv
ENV PATH /venv/bin:$PATH

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

ENTRYPOINT [ "bash", "-c", "python -Im mindsdb --config=/root/mindsdb_config.json --api=http" ]
