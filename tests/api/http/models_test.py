import os
import pytest
from tempfile import TemporaryDirectory

from mindsdb.api.http.initialize import initialize_app
from mindsdb.migrations import migrate
from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config


@pytest.fixture(scope="session", autouse=True)
def app():
    old_minds_db_con = ''
    if 'MINDSDB_DB_CON' in os.environ:
        old_minds_db_con = os.environ['MINDSDB_DB_CON']
    with TemporaryDirectory(prefix='models_test_') as temp_dir:
        db_path = 'sqlite:///' + os.path.join(temp_dir, 'mindsdb.sqlite3.db')
        # Need to change env variable for migrate module, since it calls db.init().
        os.environ['MINDSDB_DB_CON'] = db_path
        db.init()
        migrate.migrate_to_head()
        app = initialize_app(Config(), True, False)

        # Create an integration database.
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


def test_train_model(client):
    # Learning Hub home rentals model.
    create_query = '''
        CREATE MODEL mindsdb.home_rentals_model
        FROM example_db (SELECT * FROM demo_data.home_rentals)
        PREDICT rental_price
    '''
    train_data = {
        'query': create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert '201' in response.status
    created_model = response.get_json()

    expected_model = {
        'accuracy': None,
        'active': True,
        'error': None,
        'fetch_data_query': 'SELECT * FROM demo_data.home_rentals',
        'mindsdb_version': created_model['mindsdb_version'],
        'name': 'home_rentals_model',
        'predict': 'rental_price',
        'status': 'generating',
        'version': 1,
        'problem_definition': "{'target': 'rental_price'}"
    }

    assert created_model == expected_model


def test_train_model_no_query_aborts(client):
    response = client.post('/api/projects/mindsdb/models', json={}, follow_redirects=True)
    assert '400' in response.status


def test_train_model_no_project_aborts(client):
    response = client.post('/api/projects/nani/models', json={'query': ''}, follow_redirects=True)
    assert '404' in response.status


def test_train_model_invalid_query_aborts(client):
    invalid_create_query = '''
        CREATE MAWDOL mindsdb.home_rentals_model
        FRUM example_db (SELECT * FROM demo_data.home_rentals]
        PRAYDICT rental_price
    '''
    train_data = {
        'query': invalid_create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert '400' in response.status


def test_train_model_no_create_query_aborts(client):
    invalid_create_query = '''
        SELECT * FROM models
    '''
    train_data = {
        'query': invalid_create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert '400' in response.status


def test_train_model_already_exists_aborts(client):
    # Learning Hub home rentals model.
    create_query = '''
        CREATE MODEL mindsdb.home_rentals_model_duplicate
        FROM example_db (SELECT * FROM demo_data.home_rentals)
        PREDICT rental_price
    '''
    train_data = {
        'query': create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert '201' in response.status
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert '409' in response.status


def test_train_model_no_ml_handler_aborts(client):
    # Learning Hub home rentals model.
    create_query = '''
        CREATE MODEL mindsdb.home_rentals_model_no_handler
        FROM example_db (SELECT * FROM demo_data.home_rentals)
        PREDICT rental_price
        USING engine = 'vroomvroom'
    '''
    train_data = {
        'query': create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert '404' in response.status


def test_get_model_by_version(client):
    response = client.get('/api/projects/mindsdb/models/home_rentals_model.1', follow_redirects=True)
    model_ver_1 = response.get_json()
    response = client.get('/api/projects/mindsdb/models/home_rentals_model', follow_redirects=True)
    model_active_ver = response.get_json()

    expected_model = model_ver_1.copy()
    expected_model['active'] = True
    expected_model['error'] = None
    expected_model['fetch_data_query'] = 'SELECT * FROM demo_data.home_rentals'
    expected_model['name'] = 'home_rentals_model'
    expected_model['predict'] = 'rental_price'
    expected_model['version'] = 1

    assert model_ver_1 == expected_model

    assert model_active_ver == expected_model


def test_get_model_no_project_aborts(client):
    response = client.get('/api/projects/mawp/models/home_rentals_model', follow_redirects=True)
    assert '404' in response.status


def test_predict_model_no_project_aborts(client):
    response = client.post('/api/projects/mawp/models/home_rentals_model/predict', follow_redirects=True)
    assert '404' in response.status


def test_predict_model_no_model_aborts(client):
    response = client.post('/api/projects/mindsdb/models/plumbus/predict', follow_redirects=True)
    assert '404' in response.status


def test_describe_model_no_project_aborts(client):
    response = client.get('/api/projects/mawp/models/home_rentals_model/describe', follow_redirects=True)
    assert '404' in response.status


def test_describe_model_no_model_aborts(client):
    response = client.get('/api/projects/mindsdb/models/plumbus/describe', follow_redirects=True)
    assert '404' in response.status


def test_delete_model(client):
    # Learning Hub home rentals model.
    create_query = '''
        CREATE MODEL mindsdb.home_rentals_model_delete
        FROM example_db (SELECT * FROM demo_data.home_rentals)
        PREDICT rental_price
    '''
    train_data = {
        'query': create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert '201' in response.status

    response = client.delete('/api/projects/mindsdb/models/home_rentals_model_delete', follow_redirects=True)
    assert '204' in response.status

    response = client.get('/api/projects/mindsdb/models/home_rentals_model_delete', follow_redirects=True)
    assert '404' in response.status


def test_delete_model_no_project_aborts(client):
    response = client.delete('/api/projects/mawp/models/home_rentals_model', follow_redirects=True)
    assert '404' in response.status


def test_delete_model_no_model_aborts(client):
    response = client.delete('/api/projects/mindsdb/models/meeseeks', follow_redirects=True)
    assert '404' in response.status
