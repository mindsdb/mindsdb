import os

from flask import Flask, Response
from prometheus_client import generate_latest, multiprocess, CollectorRegistry

from mindsdb.utilities import log

_CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')
logger = log.getLogger(__name__)


def init_metrics(app: Flask):
    prometheus_dir = os.environ.get('PROMETHEUS_MULTIPROC_DIR', None)
    if prometheus_dir is None:
        logger.info("PROMETHEUS_MULTIPROC_DIR environment variable is not set. Metrics server won't be started.")
        return
    elif not os.path.isdir(prometheus_dir):
        os.makedirs(prometheus_dir)
    # See: https://prometheus.github.io/client_python/multiprocess/
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)

    # It's important that the PROMETHEUS_MULTIPROC_DIR env variable is set, and the dir is empty.
    @app.route('/metrics')
    def metrics():
        return Response(generate_latest(registry), mimetype=_CONTENT_TYPE_LATEST)
