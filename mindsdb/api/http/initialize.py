from distutils.version import LooseVersion
import requests
import os
import shutil
from zipfile import ZipFile
import inspect
from pathlib import Path

from flask import Flask, url_for
from flask_restx import Api
from flask_cors import CORS
import json
import boto3

from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.native.mindsdb import MindsdbNative


class Swagger_Api(Api):
    """
    This is a modification of the base Flask Restplus Api class due to the issue described here
    https://github.com/noirbizarre/flask-restplus/issues/223
    """
    @property
    def specs_url(self):
        return url_for(self.endpoint("specs"), _external=False)


def initialize_static():
    this_file_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
    static_path = Path(this_file_path).parent.joinpath('static/')
    static_path.mkdir(parents=True, exist_ok=True)

    try:
        res = requests.get('https://raw.githubusercontent.com/mindsdb/mindsdb_gui_web/master/compatible-config.json?token=AA7S27R5CPBEUKNEONQJNBC7LPBJK')
    except ConnectionError as e:
        print(f'Is no connection. {e}')
        return False

    versions = res.json()

    current_mindsdb_lv = LooseVersion(mindsdb_version)

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

    ACCESS_KEY = ""
    SECRET_KEY = ""

    try:
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        s3.download_file('mindsdb-web-builds', 'index.html', str(static_path.joinpath('index.html')))
        css_zip_path = str(static_path.joinpath('css.zip'))
        js_zip_path = str(static_path.joinpath('js.zip'))
        s3.download_file('mindsdb-web-builds', 'css-V' + gui_version_lv.vstring + '.zip', css_zip_path)
        s3.download_file('mindsdb-web-builds', 'js-V' + gui_version_lv.vstring + '.zip', js_zip_path)
    except Exception as e:
        print(f'Error during downloading files from s3: {e}')
        return False

    # unzip process
    ZipFile(js_zip_path).extractall(static_path)
    ZipFile(css_zip_path).extractall(static_path)

    os.remove(js_zip_path)
    os.remove(css_zip_path)

    shutil.move(static_path.joinpath('build', 'static', 'js'), static_path.joinpath('js'))
    shutil.move(static_path.joinpath('build', 'static', 'css'), static_path.joinpath('css'))

    shutil.rmtree(static_path.joinpath('build'))

    with open(version_txt_path, 'wt') as f:
        f.write(gui_version_lv.vstring)

    print(f'GUI version updated to {gui_version_lv.vstring}')
    return True


def initialize_flask(config):
    app = Flask(__name__, static_url_path='/static')

    app.config['SWAGGER_HOST'] = 'http://localhost:8000/mindsdb'
    authorizations = {
        'apikey': {
            'type': 'apiKey',
            'in': 'query',
            'name': 'apikey'
        }
    }
    cors_origin_list = ["http://localhost:5000", "http://localhost:3000"]
    cors = CORS(app, resources={r"/*": {"origins": cors_origin_list}})

    api = Swagger_Api(app, authorizations=authorizations, security=['apikey'], url_prefix=':8000')

    return app, api


def initialize_interfaces(config, app):
    app.default_store = DataStore(config)
    app.mindsdb_native = MindsdbNative(config)
    app.config_obj = config
