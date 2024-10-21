# This first stage produces a minimal file structure for dependencies to run `pip install .`
# This approach helps to cache dependencies and speeds up the build process by avoiding unnecessary invalidation
FROM python:3.10 as deps
WORKDIR /mindsdb

# Copy everything initially
COPY . .

# Remove unnecessary files and empty directories
RUN find ./ -type f -not -name "requirements*.txt" -exec rm -f {} + \
    && find ./ -type d -empty -delete

# Copy necessary files for installation
COPY setup.py default_handlers.txt README.md ./
COPY mindsdb/__about__.py mindsdb/

# This stage installs the dependencies using the cleaned structure from the previous stage
FROM python:3.10 as build
WORKDIR /mindsdb
ARG EXTRAS

# Configure apt to retain downloaded packages and update the package list
RUN rm -f /etc/apt/apt.conf.d/docker-clean && \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

# Install system dependencies, with caching for faster builds
RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y && \
    apt-get install -y freetds-dev # required for pymssql on arm64

# Copy installation files and install Python dependencies
COPY --from=deps /mindsdb .
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked pip install "."
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    if [ -n "$EXTRAS" ]; then pip install $EXTRAS; fi

# Finally, copy the complete mindsdb code and install the package
COPY . .
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked pip install "."

# Development image with additional dependencies
FROM build as dev
WORKDIR /mindsdb

# Retain package cache and install development dependencies
RUN rm -f /etc/apt/apt.conf.d/docker-clean && \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y && \
    apt-get install -y libpq5 freetds-bin curl

# Install development requirements
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked pip install -r requirements/requirements-dev.txt

COPY docker/mindsdb_config.release.json /root/mindsdb_config.json

ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1

EXPOSE 47334/tcp 47335/tcp 47336/tcp

ENTRYPOINT [ "sh", "-c", "python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]

# Final production image with only necessary packages
FROM python:3.10-slim
WORKDIR /mindsdb

# Configure apt for package retention and install necessary dependencies
RUN rm -f /etc/apt/apt.conf.d/docker-clean && \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=target=/var/lib/apt,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt update && apt-get upgrade -y && \
    apt-get install -y libpq5 freetds-bin curl

# Copy installed packages and configuration
COPY --link --from=build /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY docker/mindsdb_config.release.json /root/mindsdb_config.json
COPY . .

ENV PYTHONUNBUFFERED 1
ENV MINDSDB_DOCKER_ENV 1

EXPOSE 47334/tcp 47335/tcp 47336/tcp

ENTRYPOINT [ "sh", "-c", "python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb" ]
