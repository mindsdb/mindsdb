import os
import logging
import sys
import multiprocessing

from werkzeug.exceptions import HTTPException
from waitress import serve

from mindsdb.api.http.namespaces.predictor import ns_conf as predictor_ns
from mindsdb.api.http.namespaces.datasource import ns_conf as datasource_ns
from mindsdb.api.http.namespaces.util import ns_conf as utils_ns
from mindsdb.api.http.namespaces.config import ns_conf as conf_ns
from mindsdb.api.http.initialize import initialize_flask, initialize_interfaces, initialize_static
from mindsdb.utilities.config import Config


def start(config, initial=False):
    if not initial:
        print('\n\nWarning, this process should not have been started... nothing is "wrong" but it needlessly ate away a tiny bit of precious compute !\n\n')
    config = Config(config)

    if not logging.root.handlers:
        rootLogger = logging.getLogger()

        outStream = logging.StreamHandler(sys.stdout)
        outStream.addFilter(lambda record: record.levelno <= logging.INFO)
        rootLogger.addHandler(outStream)

        errStream = logging.StreamHandler(sys.stderr)
        errStream.addFilter(lambda record: record.levelno > logging.INFO)
        rootLogger.addHandler(errStream)

    initialize_static(config)

    app, api = initialize_flask(config)
    initialize_interfaces(config, app)

    api.add_namespace(predictor_ns)
    api.add_namespace(datasource_ns)
    api.add_namespace(utils_ns)
    api.add_namespace(conf_ns)

    @api.errorhandler(Exception)
    def handle_exception(e):
        # pass through HTTP errors
        if isinstance(e, HTTPException):
            return {'message': str(e)}, e.code, e.get_response().headers
        name = getattr(type(e), '__name__') or 'Unknown error'
        return {'message': f'{name}: {str(e)}'}, 500

    port = config['api']['http']['port']
    host = config['api']['http']['host']

    print(f"Start on {host}:{port}")

    server = os.environ.get('MINDSDB_DEFAULT_SERVER', 'waitress')

    if server.lower() == 'waitress':
        serve(app, port=port, host=host)
    elif server.lower() == 'flask':
        app.run(debug=False, port=port, host=host)
    elif server.lower() == 'gunicorn':
        try:
            from mindsdb.api.http.gunicorn_wrapper import StandaloneApplication
        except ImportError:
            print("Gunicorn server is not available by default. If you wish to use it, please install 'gunicorn'")
            return
        options = {
            'bind': f'{host}:{port}',
            'workers': min(max(multiprocessing.cpu_count(), 2), 3)
        }
        StandaloneApplication(app, options).run()
