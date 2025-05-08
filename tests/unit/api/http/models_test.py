from http import HTTPStatus

from tests.api.http.conftest import create_demo_db, create_dummy_ml


TEST_DB_NAME = 'dummy_db'


def test_prepare(client):
    create_demo_db(client)
    create_dummy_ml(client)


def test_train_model(client):
    # Learning Hub home rentals model.
    create_query = '''
        CREATE MODEL mindsdb.home_rentals_model
        FROM example_db (SELECT * FROM demo_data.home_rentals limit 10)
        PREDICT rental_price
        USING engine = 'dummy_ml', join_learn_process = true
    '''
    train_data = {
        'query': create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED
    created_model = response.get_json()

    expected_model = {
        'accuracy': created_model['accuracy'],
        'active': True,
        'error': None,
        'fetch_data_query': 'SELECT * FROM demo_data.home_rentals limit 10',
        'mindsdb_version': created_model['mindsdb_version'],
        'name': 'home_rentals_model',
        'predict': 'rental_price',
        'status': 'complete',
        'version': 1,
    }
    for key, value in expected_model.items():
        assert created_model[key] == value
    assert "'target': 'rental_price'" in created_model['problem_definition']


def test_train_model_no_query_aborts(client):
    response = client.post('/api/projects/mindsdb/models', json={}, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_train_model_no_project_aborts(client):
    response = client.post('/api/projects/nani/models', json={'query': ''}, follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


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
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_train_model_no_create_query_aborts(client):
    invalid_create_query = '''
        SELECT * FROM models
    '''
    train_data = {
        'query': invalid_create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_train_model_already_exists_aborts(client):
    # Learning Hub home rentals model.
    create_query = '''
        CREATE MODEL mindsdb.home_rentals_model_duplicate
        FROM example_db (SELECT * FROM demo_data.home_rentals limit 10)
        PREDICT rental_price
        USING engine = 'dummy_ml', join_learn_process = true
    '''
    train_data = {
        'query': create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CONFLICT


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
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_get_model_by_version(client):
    response = client.get('/api/projects/mindsdb/models/home_rentals_model.1', follow_redirects=True)
    model_ver_1 = response.get_json()
    response = client.get('/api/projects/mindsdb/models/home_rentals_model', follow_redirects=True)
    model_active_ver = response.get_json()

    expected_model = model_ver_1.copy()
    expected_model['active'] = True
    expected_model['error'] = None
    expected_model['fetch_data_query'] = 'SELECT * FROM demo_data.home_rentals limit 10'
    expected_model['name'] = 'home_rentals_model'
    expected_model['predict'] = 'rental_price'
    expected_model['version'] = 1

    assert model_ver_1 == expected_model
    assert model_active_ver == expected_model


def test_get_model_no_project_aborts(client):
    response = client.get('/api/projects/mawp/models/home_rentals_model', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_predict_model_no_project_aborts(client):
    response = client.post('/api/projects/mawp/models/home_rentals_model/predict', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_predict_model_no_model_aborts(client):
    response = client.post('/api/projects/mindsdb/models/plumbus/predict', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_describe_model_no_project_aborts(client):
    response = client.get('/api/projects/mawp/models/home_rentals_model/describe', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_describe_model_no_model_aborts(client):
    response = client.get('/api/projects/mindsdb/models/plumbus/describe', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_delete_model(client):
    # Learning Hub home rentals model.
    create_query = '''
        CREATE MODEL mindsdb.home_rentals_model_delete
        FROM example_db (SELECT * FROM demo_data.home_rentals limit 10)
        PREDICT rental_price
        USING engine = 'dummy_ml', join_learn_process = true
    '''
    train_data = {
        'query': create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED

    response = client.delete('/api/projects/mindsdb/models/home_rentals_model_delete', follow_redirects=True)
    assert response.status_code == HTTPStatus.NO_CONTENT

    response = client.get('/api/projects/mindsdb/models/home_rentals_model_delete', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_delete_model_no_project_aborts(client):
    response = client.delete('/api/projects/mawp/models/home_rentals_model', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_delete_model_no_model_aborts(client):
    response = client.delete('/api/projects/mindsdb/models/meeseeks', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND
