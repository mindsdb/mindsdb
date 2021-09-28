from distutils.version import LooseVersion
import requests
import os
import shutil
import threading
import webbrowser
from zipfile import ZipFile
from pathlib import Path
import traceback
import tempfile
# import concurrent.futures
from flask import Flask, url_for, make_response
from flask.json import dumps
from flask_restx import Api

from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.model.model_interface import ModelInterface
from mindsdb.utilities.ps import is_pid_listen_port, wait_func_is_true
from mindsdb.utilities.telemetry import inject_telemetry_to_static
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import get_log
from mindsdb.interfaces.storage.db import session
from mindsdb.utilities.json_encoder import CustomJSONEncoder


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
    log = get_log('http')

    try:
        res = requests.get('https://mindsdb-web-builds.s3.amazonaws.com/compatible-config.json')
    except (ConnectionError, requests.exceptions.ConnectionError) as e:
        print(f'Is no connection. {e}')
        return False
    except Exception as e:
        print(f'Is something wrong with getting compatible-config.json: {e}')
        return False

    if res.status_code != 200:
        print(f'Cant get compatible-config.json: returned status code = {res.status_code}')
        return False

    try:
        versions = res.json()
    except Exception as e:
        print(f'Cant decode compatible-config.json: {e}')
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
        log.error(f'Error in compatible-config.json structure: {e}')
        return False
    return gui_version_lv


def get_current_gui_version() -> LooseVersion:
    config = Config()
    static_path = Path(config['paths']['static'])
    version_txt_path = static_path.joinpath('version.txt')

    current_gui_version = None
    if version_txt_path.is_file():
        with open(version_txt_path, 'rt') as f:
            current_gui_version = f.readline()

    current_gui_lv = None if current_gui_version is None else LooseVersion(current_gui_version)

    return current_gui_lv


def download_gui(destignation, version):
    if isinstance(destignation, str):
        destignation = Path(destignation)
    log = get_log('http')
    css_zip_path = str(destignation.joinpath('css.zip'))
    js_zip_path = str(destignation.joinpath('js.zip'))
    media_zip_path = str(destignation.joinpath('media.zip'))
    bucket = "https://mindsdb-web-builds.s3.amazonaws.com/"

    resources = [{
        'url': bucket + 'css-V' + version + '.zip',
        'path': css_zip_path
    }, {
        'url': bucket + 'js-V' + version + '.zip',
        'path': js_zip_path
    }, {
        'url': bucket + 'indexV' + version + '.html',
        'path': str(destignation.joinpath('index.html'))
    }, {
        'url': bucket + 'favicon.ico',
        'path': str(destignation.joinpath('favicon.ico'))
    }, {
        'url': bucket + 'media.zip',
        'path': media_zip_path
    }]

    def get_resources(resource):
        response = requests.get(resource['url'])
        if response.status_code != requests.status_codes.codes.ok:
            raise Exception(f"Error {response.status_code} GET {resource['url']}")
        open(resource['path'], 'wb').write(response.content)

    try:
        for r in resources:
            get_resources(r)
    except Exception as e:
        log.error(f'Error during downloading files from s3: {e}')
        return False

    for zip_path, dir_name in [[js_zip_path, 'js'], [css_zip_path, 'css']]:
        temp_dir = destignation.joinpath(f'temp_{dir_name}')
        temp_dir.mkdir(mode=0o777, exist_ok=True, parents=True)
        ZipFile(zip_path).extractall(temp_dir)
        files_path = destignation.joinpath('static', dir_name)
        if temp_dir.joinpath('build', 'static', dir_name).is_dir():
            shutil.move(temp_dir.joinpath('build', 'static', dir_name), files_path)
            shutil.rmtree(temp_dir)
        else:
            shutil.move(temp_dir, files_path)

    static_folder = Path(destignation).joinpath('static')
    static_folder.mkdir(parents=True, exist_ok=True)
    ZipFile(media_zip_path).extractall(static_folder)

    os.remove(js_zip_path)
    os.remove(css_zip_path)
    os.remove(media_zip_path)

    version_txt_path = destignation.joinpath('version.txt')  # os.path.join(destignation, 'version.txt')
    with open(version_txt_path, 'wt') as f:
        f.write(version)

    return True

    '''
    # to make downloading faster download each resource in a separate thread
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(get_resources, r): r for r in resources}
        for future in concurrent.futures.as_completed(future_to_url):
            res = future.result()
            if res is not None:
                raise res
    '''


def initialize_static():
    success = update_static()
    session.close()
    return success


def update_static():
    ''' Update Scout files basing on compatible-config.json content.
        Files will be downloaded and updated if new version of GUI > current.
        Current GUI version stored in static/version.txt.
    '''
    config = Config()
    log = get_log('http')
    static_path = Path(config['paths']['static'])

    last_gui_version_lv = get_last_compatible_gui_version()
    current_gui_version_lv = get_current_gui_version()

    if last_gui_version_lv is False:
        return False

    if current_gui_version_lv is not None:
        if current_gui_version_lv >= last_gui_version_lv:
            return True

    log.info(f'New version of GUI available ({last_gui_version_lv.vstring}). Downloading...')

    temp_dir = tempfile.mkdtemp(prefix='mindsdb_gui_files_')
    success = download_gui(temp_dir, last_gui_version_lv.vstring)
    if success is False:
        shutil.rmtree(temp_dir)
        return False

    temp_dir_for_rm = tempfile.mkdtemp(prefix='mindsdb_gui_files_')
    shutil.rmtree(temp_dir_for_rm)
    shutil.copytree(str(static_path), temp_dir_for_rm)
    shutil.rmtree(str(static_path))
    shutil.copytree(temp_dir, str(static_path))
    shutil.rmtree(temp_dir_for_rm)

    log.info(f'GUI version updated to {last_gui_version_lv.vstring}')
    return True


def initialize_flask(config, init_static_thread, no_studio):
    # Apparently there's a bug that causes the static path not to work if it's '/' -- https://github.com/pallets/flask/issues/3134, I think '' should achieve the same thing (???)
    if no_studio:
        app = Flask(
            __name__
        )
    else:
        static_path = os.path.join(config['paths']['static'], 'static/')
        if os.path.isabs(static_path) is False:
            static_path = os.path.join(os.getcwd(), static_path)
        app = Flask(
            __name__,
            static_url_path='/static',
            static_folder=static_path
        )

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
        log = get_log('http')
        if host in ('', '0.0.0.0'):
            url = f'http://127.0.0.1:{port}/'
        else:
            url = f'http://{host}:{port}/'
        log.info(f' - GUI available at {url}')

        pid = os.getpid()
        x = threading.Thread(target=_open_webbrowser, args=(url, pid, port, init_static_thread, config['paths']['static']), daemon=True)
        x.start()

    return app, api


def initialize_interfaces(app):
    app.original_data_store = DataStore()
    app.original_model_interface = ModelInterface()
    config = Config()
    app.config_obj = config


def _open_webbrowser(url: str, pid: int, port: int, init_static_thread, static_folder):
    """Open webbrowser with url when http service is started.

    If some error then do nothing.
    """
    init_static_thread.join()
    inject_telemetry_to_static(static_folder)
    logger = get_log('http')
    try:
        is_http_active = wait_func_is_true(func=is_pid_listen_port, timeout=10,
                                           pid=pid, port=port)
        if is_http_active:
            webbrowser.open(url)
    except Exception as e:
        logger.error(f'Failed to open {url} in webbrowser with exception {e}')
        logger.error(traceback.format_exc())
    session.close()
