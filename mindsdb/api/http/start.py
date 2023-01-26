import os
import logging
import torch.multiprocessing as mp
import threading
from pathlib import Path

from werkzeug.exceptions import HTTPException
from waitress import serve
from flask import send_from_directory, request, session
from flask_compress import Compress

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.stream import ns_conf as stream_ns
from mindsdb.api.http.namespaces.config import ns_conf as conf_ns
from mindsdb.api.http.namespaces.util import ns_conf as utils_ns
from mindsdb.api.http.namespaces.file import ns_conf as file_ns
from mindsdb.api.http.namespaces.sql import ns_conf as sql_ns
from mindsdb.api.http.namespaces.analysis import ns_conf as analysis_ns
from mindsdb.api.http.namespaces.handlers import ns_conf as handlers_ns
from mindsdb.api.http.namespaces.tree import ns_conf as tree_ns
from mindsdb.api.http.namespaces.tab import ns_conf as tab_ns
from mindsdb.api.http.namespaces.projects import ns_conf as projects_ns
from mindsdb.api.nlp.nlp import ns_conf as nlp_ns
from mindsdb.api.http.initialize import initialize_flask, initialize_interfaces, initialize_static
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx


is_first_launch = False


def check_auth():
    config = Config()
    return session.get('username') == config['auth']['username']


def start(verbose, no_studio, with_nlp):
    global is_first_launch
    config = Config()

    server = os.environ.get('MINDSDB_DEFAULT_SERVER', 'waitress')
    db.init()
    log.initialize_log(config, 'http', wrap_print=True if server.lower() != 'gunicorn' else False)

    # Here is 'detection' of first launch. Assume that GUI is not downloaded.
    # Solution won't work in multiprocess mode. In future will be need to add that info to the db
    is_first_launch = Path(config['paths']['static']).joinpath('version.txt').is_file() is False

    # start static initialization in a separate thread
    init_static_thread = None
    if not no_studio:
        init_static_thread = threading.Thread(target=initialize_static)
        init_static_thread.start()

    app, api = initialize_flask(config, init_static_thread, no_studio)
    Compress(app)
    initialize_interfaces(app)

    static_root = config['paths']['static']
    if os.path.isabs(static_root) is False:
        static_root = os.path.join(os.getcwd(), static_root)
    static_root = Path(static_root)

    @app.route('/', defaults={'path': ''}, methods=['GET'])
    @app.route('/<path:path>', methods=['GET'])
    def root_index(path):
        if path.startswith('api/'):
            return {'message': 'wrong query'}, 400
        if static_root.joinpath(path).is_file():
            return send_from_directory(static_root, path)
        else:
            return send_from_directory(static_root, 'index.html')

    @app.route('/login', methods=['POST'])
    def login():
        username = request.json.get('username')
        password = request.json.get('password')
        if (
            isinstance(username, str) is False or len(username) == 0
            or isinstance(password, str) is False or len(password) == 0
        ):
            return http_error(
                400, 'Error in username or password',
                'Username and password should be string'
            )

        config = Config()
        inline_username = config['auth']['username']
        inline_password = config['auth']['password']

        if (
            username != inline_username
            or password != inline_password
        ):
            return http_error(
                401, 'Forbidden',
                'Invalid username or password'
            )

        session['username'] = username
        session.permanent = True

        return '', 200

    @app.route('/status', methods=['GET'])
    def status():
        global is_first_launch
        environment = 'local'
        config = Config()

        if config.get('cloud', False):
            environment = 'cloud'
        elif config.get('aws_marketplace', False):
            environment = 'aws_marketplace'

        auth_provider = 'local' if config['auth']['required'] else 'disabled'

        resp = {
            'environment': environment,
            'auth': {
                'active': check_auth(),
                'provider': auth_provider
            }
        }

        if is_first_launch is True:
            resp['is_first_launch'] = True
            is_first_launch = False

        return resp

    namespaces = [
        tab_ns,
        stream_ns,
        utils_ns,
        conf_ns,
        file_ns,
        sql_ns,
        analysis_ns,
        handlers_ns,
        tree_ns,
        projects_ns
    ]
    if with_nlp:
        namespaces.append(nlp_ns)

    for ns in namespaces:
        api.add_namespace(ns)

    @api.errorhandler(Exception)
    def handle_exception(e):
        log.get_log('http').error(f'http exception: {e}')
        # pass through HTTP errors
        if isinstance(e, HTTPException):
            return {'message': str(e)}, e.code, e.get_response().headers
        name = getattr(type(e), '__name__') or 'Unknown error'
        return {'message': f'{name}: {str(e)}'}, 500

    @app.teardown_appcontext
    def remove_session(*args, **kwargs):
        db.session.close()

    @app.before_request
    def before_request():
        ctx.set_default()

        # region routes where auth is required
        if (
            config['auth']['required'] is True
            and any(request.path.startswith(f'/api{ns.path}') for ns in namespaces)
            and check_auth() is False
        ):
            return http_error(
                403, 'Forbidden',
                'Authorization is required to complete the request'
            )
        # endregion

        company_id = request.headers.get('company-id')
        user_class = request.headers.get('user-class')

        if company_id is not None:
            try:
                company_id = int(company_id)
            except Exception as e:
                log.get_log('http').error(f'Cloud not parse company id: {company_id} | exception: {e}')
                company_id = None

        if user_class is not None:
            try:
                user_class = int(user_class)
            except Exception as e:
                log.get_log('http').error(f'Cloud not parse user_class: {user_class} | exception: {e}')
                user_class = 0
        else:
            user_class = 0

        ctx.company_id = company_id
        ctx.user_class = user_class

    port = config['api']['http']['port']
    host = config['api']['http']['host']

    # waiting static initialization
    if not no_studio:
        init_static_thread.join()
    if server.lower() == 'waitress':
        if host in ('', '0.0.0.0'):
            serve(app, port=port, host='*', max_request_body_size=1073741824 * 10, inbuf_overflow=1073741824 * 10)
        else:
            serve(app, port=port, host=host, max_request_body_size=1073741824 * 10, inbuf_overflow=1073741824 * 10)
    elif server.lower() == 'flask':
        # that will 'disable access' log in console
        logger = logging.getLogger('werkzeug')
        logger.setLevel(logging.WARNING)

        app.run(debug=False, port=port, host=host)
    elif server.lower() == 'gunicorn':
        try:
            from mindsdb.api.http.gunicorn_wrapper import StandaloneApplication
        except ImportError:
            print("Gunicorn server is not available by default. If you wish to use it, please install 'gunicorn'")
            return

        def post_fork(arbiter, worker):
            db.engine.dispose()

        options = {
            'bind': f'{host}:{port}',
            'workers': mp.cpu_count(),
            'timeout': 600,
            'reuse_port': True,
            'preload_app': True,
            'post_fork': post_fork,
            'threads': 4
        }
        StandaloneApplication(app, options).run()
