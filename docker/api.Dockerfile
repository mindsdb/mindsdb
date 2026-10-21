FROM python:3.12-slim AS builder

RUN pip install uv --no-cache-dir

WORKDIR /src
COPY backend/core_agent/ ./core_agent/
COPY backend/core_api/ ./core_api/

ARG ANTON_VERSION
ARG COWORK_SERVER_VERSION

# Submodule Git metadata is outside these copied source directories. Build each
# wheel with its own source-derived version before resolving their dependencies.
RUN test -n "$ANTON_VERSION" && test -n "$COWORK_SERVER_VERSION" \
    || { echo "Pass source-derived ANTON_VERSION and COWORK_SERVER_VERSION (make docker-build)." >&2; exit 1; }
RUN case "$ANTON_VERSION:$COWORK_SERVER_VERSION" in \
      2.0.0*:*|*:0.0.0*) echo "Fetch submodule tags before deriving package versions; fallback versions are refused." >&2; exit 1 ;; \
    esac
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install uv --no-cache-dir && \
    SETUPTOOLS_SCM_PRETEND_VERSION="$ANTON_VERSION" \
      /opt/venv/bin/uv build --wheel --out-dir /tmp/wheels ./core_agent && \
    SETUPTOOLS_SCM_PRETEND_VERSION="$COWORK_SERVER_VERSION" \
      /opt/venv/bin/uv build --wheel --out-dir /tmp/wheels ./core_api && \
    /opt/venv/bin/uv pip install --python /opt/venv/bin/python /tmp/wheels/*.whl

FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="cowork-api"
LABEL org.opencontainers.image.source="https://github.com/mindsdb/minds-platform"

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 -s /bin/bash cowork

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    COWORK_SERVER_HOST=0.0.0.0 \
    COWORK_SERVER_PORT=26866

RUN mkdir -p /home/cowork/.cowork && chown cowork:cowork /home/cowork/.cowork

USER cowork

EXPOSE 26866

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:26866/api/v1/health/',timeout=3).status==200 else 1)"

CMD ["cowork-server"]
