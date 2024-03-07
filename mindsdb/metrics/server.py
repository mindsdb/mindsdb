from flask import Flask
from prometheus_client import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware


def init_metrics(app: Flask):
    # Add prometheus WSGI middleware to route /metrics requests
    # See: https://prometheus.github.io/client_python/exporting/http/flask/
    # See: https://flask.palletsprojects.com/en/3.0.x/patterns/appdispatch/#combining-applications
    app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
        '/metrics': make_wsgi_app()
    })
