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
    with TemporaryDirectory(prefix='chatbots_test_') as temp_dir:
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

        # Create model to use in all tests.
        create_query = '''
        CREATE MODEL mindsdb.test_model
        FROM example_db (SELECT * FROM demo_data.home_rentals)
        PREDICT rental_price
        '''
        train_data = {
            'query': create_query
        }
        response = test_client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
        assert '201' in response.status

        yield app
    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def test_db(client):
    # Fetch all so we don't have to go through the pain of setting context attributes
    # to fetch a single database.
    all_databases_response = client.get('/api/databases', follow_redirects=True)
    all_dbs = all_databases_response.get_json()
    for database in all_dbs:
        if database['name'] == 'example_db':
            return database
    return None


def test_get_all_chatbots(client, test_db):
    response = client.get('/api/projects/mindsdb/chatbots', follow_redirects=True)
    assert '200' in response.status
    assert len(response.get_json()) == 0

    chatbot_data = {
        'chatbot': {
            'name': 'test_get_all_chatbots',
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)

    response = client.get('/api/projects/mindsdb/chatbots', follow_redirects=True)
    assert '200' in response.status
    all_chatbots = response.get_json()
    assert len(all_chatbots) == 1
    actual_chatbot = all_chatbots[0]

    expected_chatbot = {
        'name': 'test_get_all_chatbots',
        'model_name': 'test_model',
        'agent': actual_chatbot['agent'],
        'database_id': test_db['id'],
        'database': 'example_db',
        'last_error': None,
        'is_running': True,
        'params': {
            'param1': 'value1'
        },
        'created_at': actual_chatbot['created_at'],
        'id': actual_chatbot['id'],
        'project': 'mindsdb'
    }
    assert actual_chatbot == expected_chatbot


def test_get_all_chatbots_project_not_found(client):
    response = client.get('/api/projects/glorp/chatbots', follow_redirects=True)
    assert '404' in response.status


def test_get_chatbot(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_get_chatbot',
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)

    response = client.get('/api/projects/mindsdb/chatbots/test_get_chatbot', follow_redirects=True)
    assert '200' in response.status
    actual_chatbot = response.get_json()

    expected_chatbot = {
        'name': 'test_get_chatbot',
        'model_name': 'test_model',
        'agent': actual_chatbot['agent'],
        'database_id': test_db['id'],
        'database': 'example_db',
        'last_error': None,
        'is_running': True,
        'params': {
            'param1': 'value1'
        },
        'created_at': actual_chatbot['created_at'],
        'id': actual_chatbot['id'],
        'project': 'mindsdb'
    }
    assert actual_chatbot == expected_chatbot


def test_get_chatbot_not_found(client):
    response = client.get('/api/projects/mindsdb/chatbots/test_get_chatbot_not_found', follow_redirects=True)
    assert '404' in response.status


def test_get_chatbot_project_not_found(client):
    response = client.get('/api/projects/zoop/chatbots/test_get_chatbot', follow_redirects=True)
    assert '404' in response.status


def test_post_chatbot(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_post_chatbot',
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)
    assert '201' in response.status
    created_chatbot = response.get_json()

    expected_chatbot = {
        'name': 'test_post_chatbot',
        'model_name': 'test_model',
        'agent_id': created_chatbot['agent_id'],
        'database_id': test_db['id'],
        'params': {
            'param1': 'value1'
        },
        'created_at': created_chatbot['created_at'],
        'id': created_chatbot['id'],
        'project_id': created_chatbot['project_id']
    }
    assert created_chatbot == expected_chatbot


def test_post_chatbot_no_chatbot_fails(client, test_db):
    chatbot_data = {
        'name': 'test_post_chatbot_no_chatbot_fails',
        'model_name': 'test_model',
        'database_id': test_db['id'],
        'is_running': True,
        'params': {
            'param1': 'value1'
        }
    }
    response = client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)
    assert '400' in response.status


def test_post_chatbot_no_name_fails(client, test_db):
    chatbot_data = {
        'chatbot': {
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)
    assert '400' in response.status


def test_post_chatbot_no_model_name_fails(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_post_chatbot_no_model_name_fails',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)
    assert '400' in response.status


def test_post_chatbot_no_database_id_fails(client):
    chatbot_data = {
        'chatbot': {
            'name': 'test_post_chatbot_no_database_id_fails',
            'model_name': 'test_model',
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)
    assert '400' in response.status


def test_post_chatbot_model_does_not_exist_fails(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_post_chatbot_model_does_not_exist_fails',
            'model_name': 'nonexistent_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)
    assert '404' in response.status


def test_post_chatbot_project_does_not_exist_fails(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_post_chatbot_project_does_not_exist_fails',
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.post('/api/projects/bloop/chatbots', json=chatbot_data, follow_redirects=True)
    assert '404' in response.status


def test_put_chatbot_create(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_put_chatbot_create',
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.put('/api/projects/mindsdb/chatbots/test_put_chatbot_create', json=chatbot_data, follow_redirects=True)
    assert '201' in response.status
    created_chatbot = response.get_json()

    expected_chatbot = {
        'name': 'test_put_chatbot_create',
        'model_name': 'test_model',
        'agent_id': created_chatbot['agent_id'],
        'database_id': test_db['id'],
        'params': {
            'param1': 'value1'
        },
        'created_at': created_chatbot['created_at'],
        'id': created_chatbot['id'],
        'project_id': created_chatbot['project_id']
    }
    assert created_chatbot == expected_chatbot


def test_put_chatbot_update(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_put_chatbot_update',
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.put('/api/projects/mindsdb/chatbots/test_put_chatbot_update', json=chatbot_data, follow_redirects=True)
    assert '201' in response.status

    updated_chatbot_data = {
        'chatbot': {
            'params': {
                'new_param': 'new_value'
            }
        }
    }
    response = client.put('/api/projects/mindsdb/chatbots/test_put_chatbot_update', json=updated_chatbot_data, follow_redirects=True)
    assert '200' in response.status
    updated_chatbot = response.get_json()

    expected_chatbot = {
        'name': 'test_put_chatbot_update',
        'model_name': 'test_model',
        'agent_id': updated_chatbot['agent_id'],
        'database_id': test_db['id'],
        'params': {
            'param1': 'value1',
            'new_param': 'new_value'
        },
        'created_at': updated_chatbot['created_at'],
        'id': updated_chatbot['id'],
        'project_id': updated_chatbot['project_id']
    }
    assert updated_chatbot == expected_chatbot


def test_put_chatbot_no_chatbot_fails(client, test_db):
    chatbot_data = {
        'name': 'test_put_chatbot_no_chatbot_fails',
        'model_name': 'test_model',
        'database_id': test_db['id'],
        'is_running': True,
        'params': {
            'param1': 'value1'
        }
    }
    response = client.put('/api/projects/mindsdb/chatbots/test_put_chatbot_update', json=chatbot_data, follow_redirects=True)
    assert '400' in response.status


def test_put_chatbot_project_not_found(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_put_chatbot_project_not_found',
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.put('/api/projects/flumpus/chatbots/test_put_chatbot_project_not_found', json=chatbot_data, follow_redirects=True)
    assert '404' in response.status


def test_put_chatbot_model_not_found(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_put_chatbot_model_not_found',
            'model_name': 'fake_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.put('/api/projects/mindsdb/chatbots/test_put_chatbot_model_not_found', json=chatbot_data, follow_redirects=True)
    assert '404' in response.status


def test_put_chatbot_create_no_name_fails(client, test_db):
    chatbot_data = {
        'chatbot': {
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.put('/api/projects/mindsdb/chatbots/test_put_chatbot_create_no_name_fails', json=chatbot_data, follow_redirects=True)
    assert '400' in response.status


def test_put_chatbot_create_no_model_fails(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_put_chatbot_create_no_model_fails',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.put('/api/projects/mindsdb/chatbots/test_put_chatbot_create_no_model_fails', json=chatbot_data, follow_redirects=True)
    assert '400' in response.status


def test_put_chatbot_create_no_database_id_fails(client):
    chatbot_data = {
        'chatbot': {
            'name': 'test_put_chatbot_create_no_database_id_fails',
            'model_name': 'test_model',
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    response = client.put('/api/projects/mindsdb/chatbots/test_put_chatbot_create_no_database_id_fails', json=chatbot_data, follow_redirects=True)
    assert '400' in response.status


def test_delete_chatbot(client, test_db):
    chatbot_data = {
        'chatbot': {
            'name': 'test_delete_chatbot',
            'model_name': 'test_model',
            'database_id': test_db['id'],
            'is_running': True,
            'params': {
                'param1': 'value1'
            }
        }
    }
    client.post('/api/projects/mindsdb/chatbots', json=chatbot_data, follow_redirects=True)

    response = client.delete('/api/projects/mindsdb/chatbots/test_delete_chatbot', follow_redirects=True)
    assert '204' in response.status


def test_delete_chatbot_not_found(client):
    response = client.delete('/api/projects/mindsdb/chatbots/test_delete_chatbot_not_found', follow_redirects=True)
    assert '404' in response.status


def test_delete_chatbot_project_not_found(client):
    response = client.delete('/api/projects/krombopulos/chatbots/test_post_chatbot', follow_redirects=True)
    assert '404' in response.status
