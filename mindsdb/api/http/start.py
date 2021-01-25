import os
import logging
import multiprocessing
import threading
from pathlib import Path

from werkzeug.exceptions import HTTPException
from waitress import serve
from flask import send_from_directory

from mindsdb.api.http.namespaces.predictor import ns_conf as predictor_ns
from mindsdb.api.http.namespaces.datasource import ns_conf as datasource_ns
from mindsdb.api.http.namespaces.util import ns_conf as utils_ns
from mindsdb.api.http.namespaces.config import ns_conf as conf_ns
from mindsdb.api.http.initialize import initialize_flask, initialize_interfaces, initialize_static
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import initialize_log


def start(config, verbose=False):
    config = Config(config)
    if verbose:
        config['log']['level']['console'] = 'DEBUG'

    initialize_log(config, 'http', wrap_print=True)

    # start static initialization in a separate thread
    init_static_thread = threading.Thread(target=initialize_static, args=(config,))
    init_static_thread.start()

    app, api = initialize_flask(config, init_static_thread)
    initialize_interfaces(config, app)

    static_root = Path(config.paths['static'])

    @app.route('/', defaults={'path': ''}, methods=['GET'])
    @app.route('/<path:path>', methods=['GET'])
    def root_index(path):
        if path.startswith('api/'):
            return {'message': 'wrong query'}, 400
        if static_root.joinpath(path).is_file():
            return send_from_directory(config.paths['static'], path)
        else:
            return send_from_directory(config.paths['static'], 'index.html')

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

    server = os.environ.get('MINDSDB_DEFAULT_SERVER', 'waitress')

    # waiting static initialization
    init_static_thread.join()
    if server.lower() == 'waitress':
        serve(app, port=port, host=host)
    elif server.lower() == 'flask':
        # that will 'disable access' log in console
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.WARNING)

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
