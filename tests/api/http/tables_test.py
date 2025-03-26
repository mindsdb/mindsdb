from http import HTTPStatus

from tests.api.http.conftest import create_demo_db, create_dummy_db


TEST_DB_NAME = 'dummy_db'


def test_prepare(client):
    create_demo_db(client)
    create_dummy_db(client, TEST_DB_NAME)


def test_get_tables(client):
    # Get default mindsdb tables.
    response = client.get('/api/databases/mindsdb/tables', follow_redirects=True)
    all_tables = response.get_json()
    assert any(db['name'] == 'models' for db in all_tables)


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
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_create_table(client):

    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT * FROM example_db.house_sales limit 10',
            'replace': True
        }
    }
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    # Make sure we use the CREATED HTTP status code.
    assert response.status_code == HTTPStatus.CREATED
    new_table = response.get_json()

    expected_table = {
        'name': 'test_create_table_house_sales',
        'type': 'data',
    }
    assert new_table == expected_table

    # Clean up.
    client.delete(f'/api/databases/{TEST_DB_NAME}/tables/test_create_table_house_sales', follow_redirects=True)


def test_create_table_no_table_aborts(client):
    table_data = {
        'name': 'test_create_table_house_sales',
        'select': 'SELECT * FROM example_db.house_sales limit 10',
        'replace': True
    }
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_create_table_no_name_aborts(client):
    table_data = {
        'table': {
            'select': 'SELECT * FROM example_db.house_sales limit 10',
            'replace': True
        }
    }
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_create_table_no_select_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'replace': True
        }
    }
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_create_table_in_project_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT * FROM example_db.house_sales limit 10',
            'replace': True
        }
    }
    response = client.post('/api/databases/mindsdb/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_create_table_already_exists_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_already_exists_aborts',
            'select': 'SELECT * FROM example_db.house_sales limit 10',
            'replace': False
        }
    }
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CONFLICT

    # Replace table should work fine if it already exists.
    table_data['table']['replace'] = True
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED

    # Clean up.
    client.delete(f'/api/databases/{TEST_DB_NAME}/tables/test_create_table_already_exists_aborts', follow_redirects=True)


def test_create_table_invalid_select_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT AN ERROR AWWW YEAH',
            'replace': True
        }
    }
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def create_table_database_not_found_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT * FROM example_db.house_sales limit 10',
            'replace': True
        }
    }
    response = client.post('/api/databases/missingdb/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_create_table_bad_select_aborts(client):
    table_data = {
        'table': {
            'name': 'test_create_table_house_sales',
            'select': 'SELECT wattt FROM example_db.house_sales limit 10',
            'replace': True
        }
    }
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_delete_table(client):
    table_data = {
        'table': {
            'name': 'test_delete_table',
            'select': 'SELECT * FROM example_db.house_sales limit 10',
            'replace': True
        }
    }
    response = client.post(f'/api/databases/{TEST_DB_NAME}/tables', json=table_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED

    response = client.delete(f'/api/databases/{TEST_DB_NAME}/tables/test_delete_table', follow_redirects=True)
    assert response.status_code == HTTPStatus.NO_CONTENT

    response = client.get(f'/api/databases/{TEST_DB_NAME}/tables/test_delete_table', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_delete_project_table_aborts(client):
    response = client.delete('/api/databases/mindsdb/tables/models', follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_delete_table_database_not_found_aborts(client):
    response = client.delete('/api/databases/databb/tables/models', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_delete_table_not_found_aborts(client):
    response = client.delete('/api/databases/example_db/tables/nonexistent_table', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND
