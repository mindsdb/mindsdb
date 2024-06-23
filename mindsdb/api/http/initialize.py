import datetime
import mimetypes
import os
import secrets
import threading
import traceback
import webbrowser
from distutils.version import LooseVersion
from pathlib import Path

import requests
from flask import Flask, url_for, make_response, request, send_from_directory
from flask.json import dumps
from flask_compress import Compress
from flask_restx import Api
from werkzeug.exceptions import HTTPException

from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.api.http.gui import update_static
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.agents import ns_conf as agents_ns
from mindsdb.api.http.namespaces.analysis import ns_conf as analysis_ns
from mindsdb.api.http.namespaces.auth import ns_conf as auth_ns
from mindsdb.api.http.namespaces.chatbots import ns_conf as chatbots_ns
from mindsdb.api.http.namespaces.jobs import ns_conf as jobs_ns
from mindsdb.api.http.namespaces.config import ns_conf as conf_ns
from mindsdb.api.http.namespaces.databases import ns_conf as databases_ns
from mindsdb.api.http.namespaces.default import ns_conf as default_ns, check_auth
from mindsdb.api.http.namespaces.file import ns_conf as file_ns
from mindsdb.api.http.namespaces.handlers import ns_conf as handlers_ns
from mindsdb.api.http.namespaces.knowledge_bases import ns_conf as knowledge_bases_ns
from mindsdb.api.http.namespaces.models import ns_conf as models_ns
from mindsdb.api.http.namespaces.projects import ns_conf as projects_ns
from mindsdb.api.http.namespaces.skills import ns_conf as skills_ns
from mindsdb.api.http.namespaces.sql import ns_conf as sql_ns
from mindsdb.api.http.namespaces.tab import ns_conf as tab_ns
from mindsdb.api.http.namespaces.tree import ns_conf as tree_ns
from mindsdb.api.http.namespaces.views import ns_conf as views_ns
from mindsdb.api.http.namespaces.util import ns_conf as utils_ns
from mindsdb.interfaces.database.integrations import integration_controller
from mindsdb.interfaces.database.database import DatabaseController
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.interfaces.storage import db
from mindsdb.metrics.server import init_metrics
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.json_encoder import CustomJSONEncoder
from mindsdb.utilities.ps import is_pid_listen_port, wait_func_is_true
from mindsdb.utilities.telemetry import inject_telemetry_to_static

logger = log.getLogger(__name__)


class Swagger_Api(Api):
    """
    This is a modification of the base Flask Restplus Api class due to the issue described here
    https://github.com/noirbizarre/flask-restplus/issues/223
    """
    @property
    def specs_url(self):
        return url_for(self.endpoint("specs"), _external=False)


def custom_output_json(data, code, headers=None):
    resp = make_response(dumps(data), code)
    resp.headers.extend(headers or {})
    return resp


def get_last_compatible_gui_version() -> LooseVersion:
    logger.debug("Getting last compatible frontend..")
    try:
        res = requests.get('https://mindsdb-web-builds.s3.amazonaws.com/compatible-config.json', timeout=5)
    except (ConnectionError, requests.exceptions.ConnectionError) as e:
        logger.error(f"Is no connection. {e}")
        return False
    except Exception as e:
        logger.error(f"Is something wrong with getting compatible-config.json: {e}")
        return False

    if res.status_code != 200:
        logger.error(
            f"Cant get compatible-config.json: returned status code = {res.status_code}"
        )
        return False

    try:
        versions = res.json()
    except Exception as e:
        logger.error(f"Cant decode compatible-config.json: {e}")
        return False

    current_mindsdb_lv = LooseVersion(mindsdb_version)

    try:
        gui_versions = {}
        max_mindsdb_lv = None
        max_gui_lv = None
        for el in versions['mindsdb']:
            if el['mindsdb_version'] is None:
                gui_lv = LooseVersion(el['gui_version'])
            else:
                mindsdb_lv = LooseVersion(el['mindsdb_version'])
                gui_lv = LooseVersion(el['gui_version'])
                if mindsdb_lv.vstring not in gui_versions or gui_lv > gui_versions[mindsdb_lv.vstring]:
                    gui_versions[mindsdb_lv.vstring] = gui_lv
                if max_mindsdb_lv is None or max_mindsdb_lv < mindsdb_lv:
                    max_mindsdb_lv = mindsdb_lv
            if max_gui_lv is None or max_gui_lv < gui_lv:
                max_gui_lv = gui_lv

        all_mindsdb_lv = [LooseVersion(x) for x in gui_versions.keys()]
        all_mindsdb_lv.sort()

        if current_mindsdb_lv.vstring in gui_versions:
            gui_version_lv = gui_versions[current_mindsdb_lv.vstring]
        elif current_mindsdb_lv > all_mindsdb_lv[-1]:
            gui_version_lv = max_gui_lv
        else:
            lower_versions = {key: value for key, value in gui_versions.items() if LooseVersion(key) < current_mindsdb_lv}
            if len(lower_versions) == 0:
                gui_version_lv = gui_versions[all_mindsdb_lv[0].vstring]
            else:
                all_lower_versions = [LooseVersion(x) for x in lower_versions.keys()]
                gui_version_lv = gui_versions[all_lower_versions[-1].vstring]
    except Exception as e:
        logger.error(f"Error in compatible-config.json structure: {e}")
        return False

    logger.debug(f"Last compatible frontend version: {gui_version_lv}.")
    return gui_version_lv


def get_current_gui_version() -> LooseVersion:
    logger.debug("Getting current frontend version..")
    config = Config()
    static_path = Path(config['paths']['static'])
    version_txt_path = static_path.joinpath('version.txt')

    current_gui_version = None
    if version_txt_path.is_file():
        with open(version_txt_path, 'rt') as f:
            current_gui_version = f.readline()

    current_gui_lv = (
        None if current_gui_version is None else LooseVersion(current_gui_version)
    )
    logger.debug(f"Current frontend version: {current_gui_lv}.")

    return current_gui_lv


def initialize_static():
    logger.debug("Initializing static..")
    config = Config()
    last_gui_version_lv = get_last_compatible_gui_version()
    current_gui_version_lv = get_current_gui_version()
    required_gui_version = config['gui'].get('version')

    if required_gui_version is not None:
        required_gui_version_lv = LooseVersion(required_gui_version)
        success = True
        if (
            current_gui_version_lv is None
            or required_gui_version_lv != current_gui_version_lv
        ):
            logger.debug("Updating gui..")
            success = update_static(required_gui_version_lv)
    else:
        if last_gui_version_lv is False:
            return False

        # ignore versions like '23.9.2.2'
        if current_gui_version_lv is not None and len(current_gui_version_lv.version) < 3:
            if current_gui_version_lv == last_gui_version_lv:
                return True
        logger.debug("Updating gui..")
        success = update_static(last_gui_version_lv)

    db.session.close()
    return success


def initialize_app(config, no_studio):
    static_root = config['paths']['static']
    logger.debug(f"Static route: {static_root}")
    gui_exists = Path(static_root).joinpath('index.html').is_file()
    logger.debug(f"Does GUI already exist.. {'YES' if gui_exists else 'NO'}")
    init_static_thread = None
    if (
        no_studio is False
        and (
            config['gui']['autoupdate'] is True
            or gui_exists is False
        )
    ):
        init_static_thread = threading.Thread(target=initialize_static)
        init_static_thread.start()

    app, api = initialize_flask(config, init_static_thread, no_studio)
    Compress(app)
    initialize_interfaces(app)

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

    protected_namespaces = [
        tab_ns,
        utils_ns,
        conf_ns,
        file_ns,
        sql_ns,
        analysis_ns,
        handlers_ns,
        tree_ns,
        projects_ns,
        databases_ns,
        views_ns,
        models_ns,
        chatbots_ns,
        skills_ns,
        agents_ns,
        jobs_ns,
        knowledge_bases_ns
    ]

    for ns in protected_namespaces:
        api.add_namespace(ns)
    api.add_namespace(default_ns)
    api.add_namespace(auth_ns)

    @api.errorhandler(Exception)
    def handle_exception(e):
        logger.error(f"http exception: {e}")
        # pass through HTTP errors
        if isinstance(e, HTTPException):
            return {"message": str(e)}, e.code, e.get_response().headers
        name = getattr(type(e), "__name__") or "Unknown error"
        return {"message": f"{name}: {str(e)}"}, 500

    @app.teardown_appcontext
    def remove_session(*args, **kwargs):
        db.session.remove()

    @app.before_request
    def before_request():
        logger.debug(f"HTTP: {request.path}")
        ctx.set_default()
        config = Config()

        # region routes where auth is required
        if (
            config['auth']['http_auth_enabled'] is True
            and any(request.path.startswith(f'/api{ns.path}') for ns in protected_namespaces)
            and check_auth() is False
        ):
            return http_error(
                403, 'Forbidden',
                'Authorization is required to complete the request'
            )
        # endregion

        company_id = request.headers.get('company-id')
        user_class = request.headers.get('user-class')

        try:
            email_confirmed = int(request.headers.get('email-confirmed', 1))
        except ValueError:
            email_confirmed = 1

        if company_id is not None:
            try:
                company_id = int(company_id)
            except Exception as e:
                logger.error(
                    f"Cloud not parse company id: {company_id} | exception: {e}"
                )
                company_id = None

        if user_class is not None:
            try:
                user_class = int(user_class)
            except Exception as e:
                logger.error(
                    f"Cloud not parse user_class: {user_class} | exception: {e}"
                )
                user_class = 0
        else:
            user_class = 0

        ctx.company_id = company_id
        ctx.user_class = user_class
        ctx.email_confirmed = email_confirmed

    # Wait for static initialization.
    if not no_studio and init_static_thread is not None:
        init_static_thread.join()

    logger.debug("Done initializing app.")
    return app


def initialize_flask(config, init_static_thread, no_studio):
    logger.debug("Initializing flask..")
    # region required for windows https://github.com/mindsdb/mindsdb/issues/2526
    mimetypes.add_type('text/css', '.css')
    mimetypes.add_type('text/javascript', '.js')
    # endregion

    kwargs = {}
    if no_studio is not True:
        static_path = os.path.join(config['paths']['static'], 'static/')
        if os.path.isabs(static_path) is False:
            static_path = os.path.join(os.getcwd(), static_path)
        kwargs["static_url_path"] = "/static"
        kwargs["static_folder"] = static_path
        logger.debug(f"Static path: {static_path}")

    app = Flask(__name__, **kwargs)
    init_metrics(app)

    app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32))
    app.config['SESSION_COOKIE_NAME'] = 'session'
    app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(days=31)
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 60
    app.config['SWAGGER_HOST'] = 'http://localhost:8000/mindsdb'
    app.json_encoder = CustomJSONEncoder

    authorizations = {
        'apikey': {
            'type': 'session',
            'in': 'query',
            'name': 'session'
        }
    }

    logger.debug("Creating swagger API..")
    api = Swagger_Api(
        app,
        authorizations=authorizations,
        security=['apikey'],
        url_prefix=':8000',
        prefix='/api',
        doc='/doc/'
    )

    api.representations['application/json'] = custom_output_json

    port = config['api']['http']['port']
    host = config['api']['http']['host']

    # NOTE rewrite it, that hotfix to see GUI link
    if not no_studio:
        if host in ("", "0.0.0.0"):
            url = f"http://127.0.0.1:{port}/"
        else:
            url = f"http://{host}:{port}/"
        logger.info(f" - GUI available at {url}")

        pid = os.getpid()
        x = threading.Thread(target=_open_webbrowser, args=(url, pid, port, init_static_thread, config['paths']['static']), daemon=True)
        x.start()

    return app, api


def initialize_interfaces(app):
    app.integration_controller = integration_controller
    app.database_controller = DatabaseController()
    app.file_controller = FileController()
    config = Config()
    app.config_obj = config


def _open_webbrowser(url: str, pid: int, port: int, init_static_thread, static_folder):
    """Open webbrowser with url when http service is started.

    If some error then do nothing.
    """
    if init_static_thread is not None:
        init_static_thread.join()
    inject_telemetry_to_static(static_folder)
    try:
        is_http_active = wait_func_is_true(
            func=is_pid_listen_port, timeout=15, pid=pid, port=port
        )
        if is_http_active:
            webbrowser.open(url)
    except Exception as e:
        logger.error(f"Failed to open {url} in webbrowser with exception {e}")
        logger.error(traceback.format_exc())
    db.session.close()
