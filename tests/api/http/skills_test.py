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
    with TemporaryDirectory(prefix='skills_test_') as temp_dir:
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


def test_get_all_skills(client):
    response = client.get('/api/projects/mindsdb/skills', follow_redirects=True)
    assert '200' in response.status
    assert len(response.get_json()) == 0


def test_get_all_skills_project_not_found(client):
    response = client.get('/api/projects/boop/skills', follow_redirects=True)
    assert '404' in response.status


def test_create_skill(client):
    create_request = {
        'skill': {
            'name': 'test_create_skill',
            'type': 'Knowledge Base',
            'params': {
                'k1': 'v1'
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/skills', json=create_request, follow_redirects=True)
    assert '201' in create_response.status
    created_skill = create_response.get_json()

    expected_skill = {
        'id': created_skill['id'],
        'project_id': created_skill['project_id'],
        'agent_ids': [],
        'name': 'test_create_skill',
        'type': 'Knowledge Base',
        'params': {
            'k1': 'v1'
        }
    }
    assert created_skill == expected_skill


def test_get_skill(client):
    create_request = {
        'skill': {
            'name': 'test_get_skill',
            'type': 'Knowledge Base',
            'params': {
                'k1': 'v1'
            }
        }
    }

    client.post('/api/projects/mindsdb/skills', json=create_request, follow_redirects=True)

    get_response = client.get('/api/projects/mindsdb/skills/test_get_skill', follow_redirects=True)
    assert '200' in get_response.status
    skill = get_response.get_json()

    expected_skill = {
        'id': skill['id'],
        'project_id': skill['project_id'],
        'agent_ids': [],
        'name': 'test_get_skill',
        'type': 'Knowledge Base',
        'params': {
            'k1': 'v1'
        }
    }
    assert skill == expected_skill


def test_get_skill_not_found(client):
    get_response = client.get('/api/projects/mindsdb/skills/test_get_skill_not_found', follow_redirects=True)
    assert '404' in get_response.status


def test_get_skill_project_not_found(client):
    get_response = client.get('/api/projects/zoop/skills/test_get_skill', follow_redirects=True)
    assert '404' in get_response.status


def test_post_skill_no_skill(client):
    malformed_request = {
        'name': 'test_post_skill_no_skill',
        'type': 'Knowledge Base',
        'params': {
            'k1': 'v1'
        }
    }

    create_response = client.post('/api/projects/mindsdb/skills', json=malformed_request, follow_redirects=True)
    assert '400' in create_response.status


def test_post_skill_no_name(client):
    malformed_request = {
        'skill': {
            'type': 'Knowledge Base',
            'params': {
                'k1': 'v1'
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/skills', json=malformed_request, follow_redirects=True)
    assert '400' in create_response.status


def test_post_skill_no_type(client):
    malformed_request = {
        'skill': {
            'name': 'test_post_skill_no_type',
            'params': {
                'k1': 'v1'
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/skills', json=malformed_request, follow_redirects=True)
    assert '400' in create_response.status


def test_post_skill_no_params(client):
    malformed_request = {
        'skill': {
            'name': 'test_post_skill_no_params',
            'type': 'Knowledge Base',
        }
    }

    create_response = client.post('/api/projects/mindsdb/skills', json=malformed_request, follow_redirects=True)
    assert '400' in create_response.status


def test_put_skill_create(client):
    create_request = {
        'skill': {
            'name': 'test_put_skill_create',
            'type': 'Knowledge Base',
            'params': {
                'k1': 'v1'
            }
        }
    }

    create_response = client.put('/api/projects/mindsdb/skills/test_put_skill_create', json=create_request, follow_redirects=True)
    assert '201' in create_response.status
    created_skill = create_response.get_json()

    expected_skill = {
        'id': created_skill['id'],
        'project_id': created_skill['project_id'],
        'agent_ids': [],
        'name': 'test_put_skill_create',
        'type': 'Knowledge Base',
        'params': {
            'k1': 'v1'
        }
    }
    assert created_skill == expected_skill


def test_put_skill_update(client):
    create_request = {
        'skill': {
            'name': 'test_put_skill_update',
            'type': 'Knowledge Base',
            'params': {
                'k1': 'v1',
                'k2': 'v2',
            }
        }
    }

    create_response = client.put('/api/projects/mindsdb/skills/test_put_skill_update', json=create_request, follow_redirects=True)
    assert '201' in create_response.status

    update_request = {
        'skill': {
            'name': 'test_put_skill_update_new',
            'type': 'New Type',
            'params': {
                # Remove k1, change k2, add k3
                'k1': None,
                'k2': 'v2.1',
                'k3': 'v3'
            }
        }
    }
    create_response = client.put('/api/projects/mindsdb/skills/test_put_skill_update', json=update_request, follow_redirects=True)
    assert '200' in create_response.status
    updated_skill = create_response.get_json()

    expected_skill = {
        'id': updated_skill['id'],
        'project_id': updated_skill['project_id'],
        'agent_ids': [],
        'name': 'test_put_skill_update_new',
        'type': 'New Type',
        'params': {
            'k2': 'v2.1',
            'k3': 'v3'
        }
    }
    assert updated_skill == expected_skill


def test_put_skill_no_skill(client):
    malformed_request = {
        'name': 'test_put_skill_no_skill',
        'type': 'Knowledge Base',
        'params': {
            'k1': 'v1'
        }
    }

    update_response = client.put('/api/projects/mindsdb/skills/test_put_skill_no_skill', json=malformed_request, follow_redirects=True)
    assert '400' in update_response.status


def test_put_skill_no_project(client):
    update_request = {
        'skill': {
            'name': 'test_put_skill_update',
            'type': 'Knowledge Base',
            'params': {
                'k1': 'v1',
                'k2': 'v2',
            }
        }
    }

    update_response = client.put('/api/projects/goop/skills/test_put_skill_no_project', json=update_request, follow_redirects=True)
    assert '404' in update_response.status


def test_delete_skill(client):
    create_request = {
        'skill': {
            'name': 'test_delete_skill',
            'type': 'Knowledge Base',
            'params': {
                'k1': 'v1'
            }
        }
    }

    client.post('/api/projects/mindsdb/skills', json=create_request, follow_redirects=True)

    delete_response = client.delete('/api/projects/mindsdb/skills/test_delete_skill', follow_redirects=True)
    assert '204' in delete_response.status


def test_delete_skill_not_found(client):
    delete_response = client.delete('/api/projects/mindsdb/skills/test_delete_skill_not_found', follow_redirects=True)
    assert '404' in delete_response.status


def test_delete_skill_project_not_found(client):
    delete_response = client.delete('/api/projects/woop/skills/test_create_skill', follow_redirects=True)
    assert '404' in delete_response.status
