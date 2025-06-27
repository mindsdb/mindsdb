from http import HTTPStatus


def test_get_view_project_not_found_abort(client):
    response = client.get('/api/projects/zoopy/views', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_get_view_not_found(client):
    response = client.get('/api/projects/mindsdb/views/vroom', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_create_view(client):
    view_data = {
        'view': {
            'name': 'test_create_view',
            'query': 'SELECT * FROM example_db.house_sales'
        }
    }
    response = client.post('/api/projects/mindsdb/views', json=view_data, follow_redirects=True)
    # Make sure we use the CREATED HTTP status code.
    assert response.status_code == HTTPStatus.CREATED
    new_view = response.get_json()

    expected_view = {
        'name': 'test_create_view',
        'query': 'SELECT * FROM example_db.house_sales',
        'id': new_view['id']
    }

    assert new_view == expected_view


def test_create_view_project_not_found_abort(client):
    view_data = {
        'view': {
            'name': 'test_create_view',
            'query': 'SELECT * FROM example_db.house_sales'
        }
    }
    response = client.post('/api/projects/muhproject/views', json=view_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_create_view_already_exists_abort(client):
    view_data = {
        'view': {
            'name': 'test_create_view_duplicate',
            'query': 'SELECT * FROM example_db.house_sales'
        }
    }
    response = client.post('/api/projects/mindsdb/views', json=view_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED
    create_duplicate_response = client.post('/api/projects/mindsdb/views', json=view_data, follow_redirects=True)
    # Make sure we use CONFLICT status code.
    assert create_duplicate_response.status_code == HTTPStatus.CONFLICT


def test_create_view_no_view_aborts(client):
    view_data = {
        'name': 'test_create_view',
        'query': 'SELECT * FROM example_db.house_sales'
    }
    response = client.post('/api/projects/mindsdb/views', json=view_data, follow_redirects=True)
    assert '400' in response.status


def test_create_view_no_name_aborts(client):
    view_data = {
        'view': {
            'query': 'SELECT * FROM example_db.house_sales'
        }
    }
    response = client.post('/api/projects/mindsdb/views', json=view_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_create_view_no_query_aborts(client):
    view_data = {
        'view': {
            'name': 'test_create_view'
        }
    }
    response = client.post('/api/projects/mindsdb/views', json=view_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_update_view(client):
    view_data = {
        'view': {
            'name': 'test_update_view',
            'query': 'SELECT * FROM example_db.house_sales'
        }
    }

    updated_view = {
        'view': {
            'query': 'SELECT * FROM example_db.updated_house_sales'
        }
    }
    client.post('/api/projects/mindsdb/views', json=view_data, follow_redirects=True)
    response = client.put('/api/projects/mindsdb/views/test_update_view', json=updated_view, follow_redirects=True)

    assert response.status_code == HTTPStatus.OK

    updated_view = response.get_json()
    expected_view = {
        'name': 'test_update_view',
        'query': 'SELECT * FROM example_db.updated_house_sales',
        'id': updated_view['id']
    }

    assert updated_view == expected_view


def test_update_view_creates(client):
    view_data = {
        'view': {
            'query': 'SELECT * FROM example_db.house_sales'
        }
    }

    response = client.put('/api/projects/mindsdb/views/test_update_view_creates', json=view_data, follow_redirects=True)

    assert response.status_code == HTTPStatus.CREATED

    created_view = response.get_json()
    expected_view = {
        'name': 'test_update_view_creates',
        'query': 'SELECT * FROM example_db.house_sales',
        'id': created_view['id']
    }

    assert created_view == expected_view


def test_update_view_no_view_aborts(client):
    view_data = {
        'name': 'test_update_view',
        'query': 'SELECT * FROM example_db.house_sales'
    }
    response = client.put('/api/projects/mindsdb/views/test_update_view', json=view_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_delete_view(client):
    view_data = {
        'view': {
            'name': 'test_delete_view',
            'query': 'SELECT * FROM example_db.house_sales'
        }
    }
    # Delete newly created DB.
    client.post('/api/projects/mindsdb/views', json=view_data, follow_redirects=True)
    response = client.get('/api/projects/mindsdb/views/test_delete_view', follow_redirects=True)

    assert response.status_code == HTTPStatus.OK

    response = client.delete('/api/projects/mindsdb/views/test_delete_view', follow_redirects=True)

    # Make sure we return NO_CONTENT status since we don't return the deleted DB.
    assert response.status_code == HTTPStatus.NO_CONTENT

    response = client.get('/api/projects/mindsdb/views/test_delete_view', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_delete_view_does_not_exist(client):
    response = client.delete('/api/projects/mindsdb/views/florp', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_delete_view_project_not_found(client):
    response = client.delete('/api/projects/dindsmb/views/test_delete_view', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND
