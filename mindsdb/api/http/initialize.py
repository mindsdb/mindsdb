from distutils.version import LooseVersion
import requests
import os
import shutil
from zipfile import ZipFile
from pathlib import Path
import logging

from flask import Flask, url_for
from flask_restx import Api
from flask_cors import CORS

from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.interfaces.custom.custom_models import CustomModels


class Swagger_Api(Api):
    """
    This is a modification of the base Flask Restplus Api class due to the issue described here
    https://github.com/noirbizarre/flask-restplus/issues/223
    """
    @property
    def specs_url(self):
        return url_for(self.endpoint("specs"), _external=False)


def initialize_static(config):
    static_path = Path(config.paths['static'])
    static_path.mkdir(parents=True, exist_ok=True)

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
        gui_version_lv = None
        max_mindsdb_lv = None
        for el in versions['mindsdb']:
            mindsdb_lv = LooseVersion(el['mindsdb_version'])
            gui_lv = LooseVersion(el['gui_version'])
            if mindsdb_lv.vstring not in gui_versions or gui_lv > gui_versions[mindsdb_lv.vstring]:
                gui_versions[mindsdb_lv.vstring] = gui_lv
            if max_mindsdb_lv is None or max_mindsdb_lv < mindsdb_lv:
                max_mindsdb_lv = mindsdb_lv
        if current_mindsdb_lv.vstring in gui_versions:
            gui_version_lv = gui_versions[current_mindsdb_lv.vstring]
        else:
            gui_version_lv = gui_versions[max_mindsdb_lv.vstring]
    except Exception as e:
        print(f'Error in compatible-config.json structure: {e}')
        return False

    current_gui_version = None

    version_txt_path = static_path.joinpath('version.txt')
    if version_txt_path.is_file():
        with open(version_txt_path, 'rt') as f:
            current_gui_version = f.readline()
    if current_gui_version is not None:
        current_gui_lv = LooseVersion(current_gui_version)
        if current_gui_lv >= gui_version_lv:
            return True
    print('New version of GUI available. Downloading...')

    shutil.rmtree(static_path)
    static_path.mkdir(parents=True, exist_ok=True)

    try:
        css_zip_path = str(static_path.joinpath('css.zip'))
        js_zip_path = str(static_path.joinpath('js.zip'))
        media_zip_path = str(static_path.joinpath('media.zip'))
        bucket = "https://mindsdb-web-builds.s3.amazonaws.com/"

        cssZip = requests.get(bucket + 'css-V' + gui_version_lv.vstring + '.zip')
        open(css_zip_path, 'wb').write(cssZip.content)

        jsZip = requests.get(bucket + 'js-V' + gui_version_lv.vstring + '.zip')
        open(js_zip_path, 'wb').write(jsZip.content)

        indexFile = requests.get(bucket + 'indexV' + gui_version_lv.vstring + '.html')
        open(str(static_path.joinpath('index.html')), 'wb').write(indexFile.content)

        # Common resource
        faviconFile = requests.get(bucket + 'favicon.ico')
        open(str(static_path.joinpath('favicon.ico')), 'wb').write(faviconFile.content)

        mediaZip = requests.get(bucket + 'media.zip')
        open(media_zip_path, 'wb').write(mediaZip.content)
    except Exception as e:
        print(f'Error during downloading files from s3: {e}')
        return False

    # unzip process
    ZipFile(js_zip_path).extractall(static_path)
    ZipFile(css_zip_path).extractall(static_path)
    ZipFile(media_zip_path).extractall(static_path)

    os.remove(js_zip_path)
    os.remove(css_zip_path)
    os.remove(media_zip_path)

    shutil.move(static_path.joinpath('build', 'static', 'js'), static_path.joinpath('js'))
    shutil.move(static_path.joinpath('build', 'static', 'css'), static_path.joinpath('css'))

    shutil.rmtree(static_path.joinpath('build'))

    with open(version_txt_path, 'wt') as f:
        f.write(gui_version_lv.vstring)

    print(f'GUI version updated to {gui_version_lv.vstring}')
    return True


def initialize_flask(config):
    app = Flask(
        __name__,
        static_url_path='/static',
        static_folder=config.paths['static']
    )

    app.config['SWAGGER_HOST'] = 'http://localhost:8000/mindsdb'
    authorizations = {
        'apikey': {
            'type': 'apiKey',
            'in': 'query',
            'name': 'apikey'
        }
    }

    port = config['api']['http']['port']
    host = config['api']['http']['host']
    hosts = ['0.0.0.0', 'localhost', '127.0.0.1']
    if host not in hosts:
        hosts.append(host)
    cors_origin_list = [f'http://{h}:{port}' for h in hosts]

    if 'MINDSDB_CORS_PORT' in os.environ:
        ports = os.environ['MINDSDB_CORS_PORT'].strip('[]').split(',')
        ports = [f'http://{host}:{p}' for p in ports]
        cors_origin_list.extend(ports)

    CORS(app, resources={r"/*": {"origins": cors_origin_list}})

    api = Swagger_Api(app, authorizations=authorizations, security=['apikey'], url_prefix=':8000')

    # NOTE rewrite it, that hotfix to see GUI link
    log = logging.getLogger('mindsdb.http')
    log.error(f' - GUI available at http://{host}:{port}/static/index.html')

    return app, api


def initialize_interfaces(config, app):
    app.default_store = DataStore(config)
    app.mindsdb_native = MindsdbNative(config)
    app.custom_models = CustomModels(config)
    app.config_obj = config
