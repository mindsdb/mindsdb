import os

from flask import Flask, Response
from prometheus_client import generate_latest, multiprocess, CollectorRegistry

from mindsdb.utilities import log

_CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')
logger = log.getLogger(__name__)


def init_metrics(app: Flask):
    if os.environ.get('PROMETHEUS_MULTIPROC_DIR', None) is None:
        logger.warning('PROMETHEUS_MULTIPROC_DIR environment variable is not set and is needed for metrics server.')
        return
    # See: https://prometheus.github.io/client_python/multiprocess/
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)

    # It's important that the PROMETHEUS_MULTIPROC_DIR env variable is set, and the dir is empty.
    @app.route('/metrics')
    def metrics():
        return Response(generate_latest(registry), mimetype=_CONTENT_TYPE_LATEST)
