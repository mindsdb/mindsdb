import os
import mindsdb
import logging
import sys

from werkzeug.exceptions import HTTPException

from mindsdb.api.http.namespaces.predictor import ns_conf as predictor_ns
from mindsdb.api.http.namespaces.datasource import ns_conf as datasource_ns
from mindsdb.api.http.namespaces.util import ns_conf as utils_ns
from mindsdb.api.http.namespaces.config import ns_conf as conf_ns
from mindsdb.api.http.initialize import initialize_flask, initialize_interfaces
from mindsdb.utilities.config import Config


def start(config, initial=False):
    if not initial:
        print('\n\nWarning, this process should not have been started... nothing is "wrong" but it needlessly ate away a tiny bit of precious comute !\n\n')
    config = Config(config)
    debug = False

    if not logging.root.handlers:
        rootLogger = logging.getLogger()

        outStream = logging.StreamHandler(sys.stdout)
        outStream.addFilter(lambda record: record.levelno <= logging.INFO)
        rootLogger.addHandler(outStream)

        errStream = logging.StreamHandler(sys.stderr)
        errStream.addFilter(lambda record: record.levelno > logging.INFO)
        rootLogger.addHandler(errStream)

    mindsdb.CONFIG.MINDSDB_DATASOURCES_PATH = os.path.join(mindsdb.CONFIG.MINDSDB_STORAGE_PATH, 'datasources')
    mindsdb.CONFIG.MINDSDB_TEMP_PATH = os.path.join(mindsdb.CONFIG.MINDSDB_STORAGE_PATH, 'tmp')

    os.makedirs(mindsdb.CONFIG.MINDSDB_STORAGE_PATH, exist_ok=True)
    os.makedirs(mindsdb.CONFIG.MINDSDB_DATASOURCES_PATH, exist_ok=True)
    os.makedirs(mindsdb.CONFIG.MINDSDB_TEMP_PATH, exist_ok=True)

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

    @app.after_request
    def add_header(resp):
        resp.headers['Access-Control-Allow-Origin'] = 'http://localhost:8000'
        resp.headers['Vary'] = 'Origin'
        return resp

    print(f"Start on {config['api']['http']['host']}:{config['api']['http']['port']}")
    app.run(debug=debug, port=config['api']['http']['port'], host=config['api']['http']['host'])


if __name__ == '__main__':
    start()
