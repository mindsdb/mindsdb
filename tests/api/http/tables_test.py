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

        # Create an integration database.
        # Make sure to clean up any tables created since they will be created in the actual demo DB.
        test_client = app.test_client()
        # From Learning Hub.
        example_db_data = {
            'database': {
                'name': 'example_db',
                'engine': 'postgres',
                'parameters': {
                    "user": "demo_user",
                    "password": "demo_password",
                    "host": "3.220.66.106",
                    "port": "5432",
                    "database": "demo"
                }
            }
        }
        response = test_client.post('/api/databases', json=example_db_data, follow_redirects=True)
        assert '201' in response.status

        yield app
    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app):
    return app.test_client()


def test_get_tables(client):
    # Get default mindsdb tables.
    response = client.get('/api/databases/mindsdb/tables', follow_redirects=True)
    all_tables = response.get_json()
    assert any(db['name'] == 'models' for db in all_tables)
    assert any(db['name'] == 'models_versions' for db in all_tables)


def test_get_table(client):
    # Get default mindsdb models.
    response = client.get('/api/databases/mindsdb/tables/models', follow_redirects=True)
    table = response.get_json()
    expected_table = {
        'name': 'models',
        'type': 'data'
    }
    assert table == expected_table


def test_get_table_not_found(client):
    # Get default mindsdb models.
    response = client.get('/api/databases/mindsdb/tables/bloop', follow_redirects=True)
    assert '404' in response.status


def test_create_table(client):

    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT * FROM example_db.house_sales',
            'replace': True
        }
    }
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    # Make sure we use the CREATED HTTP status code.
    assert '201' in response.status
    new_table = response.get_json()

    expected_table = {
        'name': 'test_create_table_house_sales',
        'type': 'data',
    }
    assert new_table == expected_table

    # Clean up.
    client.delete('/api/databases/example_db/tables/test_create_table_house_sales', follow_redirects=True)


def test_create_table_no_table_aborts(client):
    table_data = {
        'name': 'test_create_table_house_sales',
        'select': 'SELECT * FROM example_db.house_sales',
        'replace': True
    }
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '400' in response.status


def test_create_table_no_name_aborts(client):
    table_data = {
        'table': {
            'select': 'SELECT * FROM example_db.house_sales',
            'replace': True
        }
    }
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '400' in response.status


def test_create_table_no_select_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'replace': True
        }
    }
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '400' in response.status


def test_create_table_in_project_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT * FROM example_db.house_sales',
            'replace': True
        }
    }
    response = client.post('/api/databases/mindsdb/tables', json=table_data, follow_redirects=True)
    assert '400' in response.status


def test_create_table_already_exists_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_already_exists_aborts',
            'select': 'SELECT * FROM example_db.house_sales',
            'replace': False
        }
    }
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '201' in response.status
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '409' in response.status

    # Replace table should work fine if it already exists.
    table_data['table']['replace'] = True
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '201' in response.status

    # Clean up.
    client.delete('/api/databases/example_db/tables/test_create_table_already_exists_aborts', follow_redirects=True)


def test_create_table_invalid_select_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT AN ERROR AWWW YEAH',
            'replace': True
        }
    }
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '400' in response.status


def create_table_database_not_found_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT * FROM example_db.house_sales',
            'replace': True
        }
    }
    response = client.post('/api/databases/missingdb/tables', json=table_data, follow_redirects=True)
    assert '404' in response.status


def test_create_table_bad_select_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT wattt FROM example_db.house_sales',
            'replace': True
        }
    }
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '400' in response.status


def test_delete_table(client):
    table_data = {
        'table': {
            'name': 'test_delete_table',
            'select': 'SELECT * FROM example_db.house_sales',
            'replace': True
        }
    }
    response = client.post('/api/databases/example_db/tables', json=table_data, follow_redirects=True)
    assert '201' in response.status

    response = client.delete('/api/databases/example_db/tables/test_delete_table', follow_redirects=True)
    assert '204' in response.status

    response = client.get('/api/databases/example_db/tables/test_delete_table', follow_redirects=True)
    assert '404' in response.status


def test_delete_project_table_aborts(client):
    response = client.delete('/api/databases/mindsdb/tables/models', follow_redirects=True)
    assert '400' in response.status


def test_delete_table_database_not_found_aborts(client):
    response = client.delete('/api/databases/databb/tables/models', follow_redirects=True)
    assert '404' in response.status


def test_delete_table_not_found_aborts(client):
    response = client.delete('/api/databases/example_db/tables/nonexistent_table', follow_redirects=True)
    assert '404' in response.status
