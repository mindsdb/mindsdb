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
    with TemporaryDirectory(prefix='agents_test_') as temp_dir:
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

        # Create skills to use in all tests.
        create_request = {
            'skill': {
                'name': 'test_skill',
                'type': 'Knowledge Base',
                'params': {
                    'k1': 'v1'
                }
            }
        }

        create_response = test_client.post('/api/projects/mindsdb/skills', json=create_request, follow_redirects=True)
        assert '201' in create_response.status

        create_request = {
            'skill': {
                'name': 'test_skill_2',
                'type': 'Knowledge Base',
                'params': {
                    'k1': 'v1'
                }
            }
        }

        create_response = test_client.post('/api/projects/mindsdb/skills', json=create_request, follow_redirects=True)
        assert '201' in create_response.status

        yield app
    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app):
    return app.test_client()


def test_post_agent(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'skills': ['test_skill']
        }
    }

    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    created_agent = create_response.get_json()

    expected_agent = {
        'name': 'test_post_agent',
        'model_name': 'test_model',
        'params': {
            'k1': 'v1'
        },
        'skills': created_agent['skills'],
        'id': created_agent['id'],
        'project_id': created_agent['project_id'],
        'created_at': created_agent['created_at'],
        'updated_at': created_agent['updated_at']
    }

    assert created_agent == expected_agent
    assert len(created_agent['skills']) == 1
    assert created_agent['skills'][0]['agent_ids'] == [created_agent['id']]


def test_post_agent_no_agent(client):
    create_request = {
        'name': 'test_post_agent_no_agent',
        'model_name': 'test_model',
        'params': {
            'k1': 'v1'
        },
        'skills': ['test_skill']
    }
    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert '400' in create_response.status


def test_post_agent_no_name(client):
    create_request = {
        'agent': {
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'skills': ['test_skill']
        }
    }
    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert '400' in create_response.status


def test_post_agent_no_model_name(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent_no_model_name',
            'params': {
                'k1': 'v1'
            },
            'skills': ['test_skill']
        }
    }
    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert '400' in create_response.status


def test_post_agent_project_not_found(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent_no_model_name',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'skills': ['test_skill']
        }
    }
    create_response = client.post('/api/projects/womp/agents', json=create_request, follow_redirects=True)
    assert '404' in create_response.status


def test_post_agent_model_not_found(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent_model_not_found',
            'model_name': 'not_the_model_youre_looking_for',
            'params': {
                'k1': 'v1'
            },
            'skills': ['test_skill']
        }
    }
    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert '404' in create_response.status


def test_post_agent_skill_not_found(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent_skill_not_found',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'skills': ['overpowered_skill']
        }
    }
    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert '404' in create_response.status


def test_get_agents(client):
    create_request = {
        'agent': {
            'name': 'test_get_agents',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'skills': ['test_skill']
        }
    }

    client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)

    get_response = client.get('/api/projects/mindsdb/agents', follow_redirects=True)
    assert '200' in get_response.status
    all_agents = get_response.get_json()
    assert len(all_agents) > 0


def test_get_agents_project_not_found(client):
    get_response = client.get('/api/projects/bloop/agents', follow_redirects=True)
    assert '404' in get_response.status


def test_get_agent(client):
    create_request = {
        'agent': {
            'name': 'test_get_agent',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'skills': ['test_skill']
        }
    }

    client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)

    get_response = client.get('/api/projects/mindsdb/agents/test_get_agent', follow_redirects=True)
    assert '200' in get_response.status
    agent = get_response.get_json()

    expected_agent = {
        'name': 'test_get_agent',
        'model_name': 'test_model',
        'params': {
            'k1': 'v1'
        },
        'skills': agent['skills'],
        'id': agent['id'],
        'project_id': agent['project_id'],
        'created_at': agent['created_at'],
        'updated_at': agent['updated_at']
    }

    assert agent == expected_agent


def test_get_agent_project_not_found(client):
    get_response = client.get('/api/projects/bloop/agents/test_get_agent', follow_redirects=True)
    assert '404' in get_response.status


def test_put_agent_create(client):
    create_request = {
        'agent': {
            'name': 'test_put_agent_create',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'skills': ['test_skill']
        }
    }

    put_response = client.put('/api/projects/mindsdb/agents/test_put_agent_create', json=create_request, follow_redirects=True)
    assert '201' in put_response.status

    created_agent = put_response.get_json()

    expected_agent = {
        'name': 'test_put_agent_create',
        'model_name': 'test_model',
        'params': {
            'k1': 'v1'
        },
        'skills': created_agent['skills'],
        'id': created_agent['id'],
        'project_id': created_agent['project_id'],
        'created_at': created_agent['created_at'],
        'updated_at': created_agent['updated_at']
    }

    assert created_agent == expected_agent


def test_put_agent_update(client):
    create_request = {
        'agent': {
            'name': 'test_put_agent_update',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1',
                'k2': 'v2'
            },
            'skills': ['test_skill']
        }
    }

    response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert '201' in response.status

    update_request = {
        'agent': {
            'params': {
                'k1': 'v1.1',
                'k2': None,
                'k3': 'v3'
            },
            'skills_to_add': ['test_skill_2'],
            'skills_to_remove': ['test_skill']
        }
    }

    update_response = client.put('/api/projects/mindsdb/agents/test_put_agent_update', json=update_request, follow_redirects=True)
    updated_agent = update_response.get_json()

    expected_agent = {
        'name': 'test_put_agent_update',
        'model_name': 'test_model',
        'params': {
            'k1': 'v1.1',
            'k3': 'v3'
        },
        'skills': updated_agent['skills'],
        'id': updated_agent['id'],
        'project_id': updated_agent['project_id'],
        'created_at': updated_agent['created_at'],
        'updated_at': updated_agent['updated_at']
    }

    assert updated_agent == expected_agent

    assert len(updated_agent['skills']) == 1
    skill = updated_agent['skills'][0]
    assert skill['name'] == 'test_skill_2'


def test_put_agent_no_agent(client):
    create_request = {
        'name': 'test_put_agent_no_agent',
        'model_name': 'test_model',
        'params': {
            'k1': 'v1',
            'k2': 'v2'
        },
        'skills': ['test_skill']
    }

    response = client.put('/api/projects/mindsdb/agents/test_put_agent_no_agent', json=create_request, follow_redirects=True)
    assert '400' in response.status


def test_put_agent_model_not_found(client):
    create_request = {
        'agent': {
            'name': 'test_put_agent_model_not_found',
            'model_name': 'oopsy_daisy',
            'params': {
                'k1': 'v1',
                'k2': 'v2'
            },
            'skills': ['test_skill']
        }
    }

    response = client.put('/api/projects/mindsdb/agents/test_put_agent_model_not_found', json=create_request, follow_redirects=True)
    assert '404' in response.status


def test_delete_agent(client):
    create_request = {
        'agent': {
            'name': 'test_delete_agent',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1',
                'k2': 'v2'
            },
            'skills': ['test_skill']
        }
    }

    client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)

    delete_response = client.delete('/api/projects/mindsdb/agents/test_delete_agent', follow_redirects=True)
    assert '204' in delete_response.status

    get_response = client.get('/api/projects/mindsdb/agents/test_delete_agent', follow_redirects=True)
    assert '404' in get_response.status


def test_delete_agent_project_not_found(client):
    delete_response = client.delete('/api/projects/doop/agents/test_post_agent', follow_redirects=True)
    assert '404' in delete_response.status


def test_delete_agent_not_found(client):
    delete_response = client.delete('/api/projects/mindsdb/agents/test_delete_agent_not_found', follow_redirects=True)
    assert '404' in delete_response.status
