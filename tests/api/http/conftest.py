import os
from http import HTTPStatus
from tempfile import TemporaryDirectory

import pytest
from flask.testing import FlaskClient
from flask.app import Flask

from mindsdb.api.http.initialize import initialize_app
from mindsdb.migrations import migrate
from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import config


@pytest.fixture(scope="session", autouse=True)
def app():
    old_minds_db_con = ''
    if 'MINDSDB_DB_CON' in os.environ:
        old_minds_db_con = os.environ['MINDSDB_DB_CON']
    with TemporaryDirectory(prefix='skills_test_') as temp_dir:
        db_path = 'sqlite:///' + os.path.join(temp_dir, 'mindsdb.sqlite3.db')
        # Need to change env variable for migrate module, since it calls db.init().
        os.environ['MINDSDB_DB_CON'] = db_path
        config.prepare_env_config()
        config.merge_configs()
        db.init()
        migrate.migrate_to_head()
        app = initialize_app(config, True)
        app._mindsdb_temp_dir = temp_dir
        yield app

    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app: Flask) -> FlaskClient:
    return app.test_client()


def create_dummy_db(client: FlaskClient, db_name: str):
    temp_dir = client.application._mindsdb_temp_dir
    dummy_data_db_path = os.path.join(temp_dir, '_dummy_data_db')
    response = client.post(
        '/api/sql/query',
        json={
            'query': f'''
                create database {db_name}
                with ENGINE = "dummy_data"
                PARAMETERS = {{"db_path": "{dummy_data_db_path}"}}'''
        }
    )
    assert response.status_code == HTTPStatus.OK


def create_demo_db(client: FlaskClient):
    example_db_data = {
        'database': {
            'name': 'example_db',
            'engine': 'postgres',
            'parameters': {
                "user": "demo_user",
                "password": "demo_password",
                "host": "samples.mindsdb.com",
                "port": "5432",
                "database": "demo",
                "schema": "demo_data"
            }
        }
    }
    response = client.post('/api/databases', json=example_db_data, follow_redirects=True)
    assert '201' in response.status
