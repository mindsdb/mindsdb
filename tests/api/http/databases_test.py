import os
import pytest
from tempfile import TemporaryDirectory

from mindsdb.api.http.initialize import initialize_app
from mindsdb.interfaces.storage import db
from mindsdb.migrations import migrate
from mindsdb.utilities.config import Config


@pytest.fixture(scope="session", autouse=True)
def app():
    old_minds_db_con = ''
    if 'MINDSDB_DB_CON' in os.environ:
        old_minds_db_con = os.environ['MINDSDB_DB_CON']
    with TemporaryDirectory(prefix='projects_test_') as temp_dir:
        db_path = 'sqlite:///' + os.path.join(temp_dir, 'mindsdb.sqlite3.db')
        # Need to change env variable for migrate module, since it calls db.init().
        os.environ['MINDSDB_DB_CON'] = db_path
        db.init()
        migrate.migrate_to_head()
        app = initialize_app(Config(), True, False)

        yield app
    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app):
    return app.test_client()


def test_get_databases(client):
    response = client.get('/api/databases', follow_redirects=True)
    all_databases = response.get_json()
    # Should contain default project and information schema.
    assert len(all_databases) == 2
    assert any(db['name'] == 'information_schema' for db in all_databases)
    assert any(db['name'] == 'mindsdb' for db in all_databases)


def test_get_database(client):
    # Get default mindsdb project.
    response = client.get('/api/databases/mindsdb', follow_redirects=True)
    mindsdb_database = response.get_json()
    expected_db = {
        'name': 'mindsdb',
        'engine': None,
        'type': 'project',
        'id': mindsdb_database['id']
    }

    assert mindsdb_database == expected_db

    # Get a newly created integration.
    integration_data = {
        'database': {
            'name': 'test_get_database',
            'engine': 'postgres',
            'parameters': {
                'user': 'ricky_sanchez',
                'password': 'florpglorp'
            }
        }
    }
    response = client.post('/api/databases', json=integration_data, follow_redirects=True)
    response = client.get('/api/databases/test_get_database', follow_redirects=True)

    integration_db = response.get_json()
    expected_db = {
        'name': 'test_get_database',
        'type': 'data',
        'engine': 'postgres',
        'connection_data': {
            'user': 'ricky_sanchez',
            'password': 'florpglorp'
        },
        'id': integration_db['id'],
        'date_last_update': integration_db['date_last_update'],
    }

    assert integration_db == expected_db


def test_create_database(client):
    mindsdb_data = {
        'database': {
            'name': 'test_postgres',
            'engine': 'postgres',
            'parameters': {}
        }
    }
    response = client.post('/api/databases', json=mindsdb_data, follow_redirects=True)
    # Make sure we use the CREATED HTTP status code.
    assert '201' in response.status
    new_db = response.get_json()

    expected_db = {
        'name': 'test_postgres',
        'engine': 'postgres',
        'type': 'data',
        'id': new_db['id']
    }
    assert new_db == expected_db


def test_create_database_already_exists_abort(client):
    mindsdb_data = {
        'database': {
            'name': 'test_duplicate',
            'engine': 'postgres',
            'parameters': {}
        }
    }
    response = client.post('/api/databases', json=mindsdb_data, follow_redirects=True)
    assert '201' in response.status
    create_duplicate_response = client.post('/api/databases', json=mindsdb_data, follow_redirects=True)
    # Make sure we use CONFLICT status code.
    assert '409' in create_duplicate_response.status


def test_create_database_no_database_aborts(client):
    mindsdb_data = {
        'name': 'test_postgres',
        'engine': 'postgres',
        'parameters': {}
    }
    response = client.post('/api/databases', json=mindsdb_data, follow_redirects=True)
    assert '400' in response.status


def test_create_database_no_name_aborts(client):
    mindsdb_data = {
        'database': {
            'engine': 'postgres',
            'parameters': {}
        }
    }
    response = client.post('/api/databases', json=mindsdb_data, follow_redirects=True)
    assert '400' in response.status


def test_create_database_no_engine_aborts(client):
    mindsdb_data = {
        'database': {
            'name': 'test_postgres',
            'parameters': {}
        }
    }
    response = client.post('/api/databases', json=mindsdb_data, follow_redirects=True)
    assert '400' in response.status


def test_update_database_creates_database(client):
    database_data = {
        'database': {
            'name': 'test_update_creates',
            'engine': 'postgres',
            'parameters': {}
        }
    }
    response = client.put('/api/databases/test_update_creates', json=database_data, follow_redirects=True)
    # Make sure we use the CREATED HTTP status code.
    assert '201' in response.status
    new_db = response.get_json()

    expected_db = {
        'name': 'test_update_creates',
        'engine': 'postgres',
        'type': 'data',
        'id': new_db['id']
    }
    assert new_db == expected_db


def test_update_database(client):
    database_data = {
        'database': {
            'name': 'test_update',
            'engine': 'postgres',
            'parameters': {}
        }
    }

    updated_data = {
        'database': {
            'parameters': {
                'user': 'bearO',
                'password': 'destroydestroydestroy'
            }
        }
    }
    client.post('/api/databases', json=database_data, follow_redirects=True)
    response = client.put('/api/databases/test_update', json=updated_data, follow_redirects=True)

    assert '200' in response.status

    updated_db = response.get_json()
    expected_db = {
        'name': 'test_update',
        'engine': 'postgres',
        'type': 'data',
        'connection_data': {
            'user': 'bearO',
            'password': 'destroydestroydestroy'
        },
        'id': updated_db['id'],
        'date_last_update': updated_db['date_last_update'],
    }

    assert updated_db == expected_db


def test_update_database_no_database_aborts(client):
    mindsdb_data = {
        'name': 'test_postgres',
        'engine': 'postgres',
        'parameters': {}
    }
    response = client.put('/api/databases/test_postgres', json=mindsdb_data, follow_redirects=True)
    assert '400' in response.status


def test_delete_database(client):
    mindsdb_data = {
        'database': {
            'name': 'test_delete',
            'engine': 'postgres',
            'parameters': {}
        }
    }
    # Delete newly created DB.
    client.post('/api/databases', json=mindsdb_data, follow_redirects=True)
    response = client.get('/api/databases/test_delete', follow_redirects=True)

    assert '200' in response.status

    response = client.delete('/api/databases/test_delete', follow_redirects=True)

    # Make sure we return NO_CONTENT status since we don't return the deleted DB.
    assert '204' in response.status

    response = client.get('/api/databases/test_delete', follow_redirects=True)
    assert '404' in response.status


def test_delete_database_does_not_exist(client):
    response = client.delete('/api/databases/batadase', follow_redirects=True)
    assert '404' in response.status


def test_delete_system_database(client):
    response = client.delete('/api/databases/information_schema', follow_redirects=True)
    assert '400' in response.status
