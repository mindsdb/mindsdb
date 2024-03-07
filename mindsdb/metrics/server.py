from flask import Flask, Response
from prometheus_client import generate_latest, multiprocess, CollectorRegistry


# See: https://prometheus.github.io/client_python/multiprocess/
registry = CollectorRegistry()
multiprocess.MultiProcessCollector(registry)
_CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')

def init_metrics(app: Flask):
    # It's important that the PROMETHEUS_MULTIPROC_DIR env variable is set, and the dir is empty.
    @app.route('/metrics')
    def metrics():
        return Response(generate_latest(registry), mimetype=_CONTENT_TYPE_LATEST)
