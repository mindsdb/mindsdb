from http import HTTPStatus


def _clear_skill(skill):
    """del keys that can not be compared"""
    exclude_keys = ['created_at', 'metadata']
    for key in exclude_keys:
        if key in skill:
            del skill[key]
    return skill


def test_get_all_skills(client):
    response = client.get('/api/projects/mindsdb/skills', follow_redirects=True)
    assert response.status_code == HTTPStatus.OK
    assert len(response.get_json()) == 0


def test_get_all_skills_project_not_found(client):
    response = client.get('/api/projects/boop/skills', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND


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
    assert create_response.status_code == HTTPStatus.CREATED
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
    assert _clear_skill(created_skill) == expected_skill


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
    assert get_response.status_code == HTTPStatus.OK
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
    assert _clear_skill(skill) == expected_skill


def test_get_skill_not_found(client):
    get_response = client.get('/api/projects/mindsdb/skills/test_get_skill_not_found', follow_redirects=True)
    assert get_response.status_code == HTTPStatus.NOT_FOUND


def test_get_skill_project_not_found(client):
    get_response = client.get('/api/projects/zoop/skills/test_get_skill', follow_redirects=True)
    assert get_response.status_code == HTTPStatus.NOT_FOUND


def test_post_skill_no_skill(client):
    malformed_request = {
        'name': 'test_post_skill_no_skill',
        'type': 'Knowledge Base',
        'params': {
            'k1': 'v1'
        }
    }

    create_response = client.post('/api/projects/mindsdb/skills', json=malformed_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.BAD_REQUEST


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
    assert create_response.status_code == HTTPStatus.BAD_REQUEST


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
    assert create_response.status_code == HTTPStatus.BAD_REQUEST


def test_post_skill_no_params(client):
    malformed_request = {
        'skill': {
            'name': 'test_post_skill_no_params',
            'type': 'Knowledge Base',
        }
    }

    create_response = client.post('/api/projects/mindsdb/skills', json=malformed_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.BAD_REQUEST


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
    assert create_response.status_code == HTTPStatus.CREATED
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
    assert _clear_skill(created_skill) == expected_skill


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
    assert create_response.status_code == HTTPStatus.CREATED

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
    assert create_response.status_code == HTTPStatus.OK
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
    assert _clear_skill(updated_skill) == expected_skill


def test_put_skill_no_skill(client):
    malformed_request = {
        'name': 'test_put_skill_no_skill',
        'type': 'Knowledge Base',
        'params': {
            'k1': 'v1'
        }
    }

    update_response = client.put('/api/projects/mindsdb/skills/test_put_skill_no_skill', json=malformed_request, follow_redirects=True)
    assert update_response.status_code == HTTPStatus.BAD_REQUEST


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
    assert update_response.status_code == HTTPStatus.NOT_FOUND


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
    assert delete_response.status_code == HTTPStatus.NO_CONTENT


def test_delete_skill_not_found(client):
    delete_response = client.delete('/api/projects/mindsdb/skills/test_delete_skill_not_found', follow_redirects=True)
    assert delete_response.status_code == HTTPStatus.NOT_FOUND


def test_delete_skill_project_not_found(client):
    delete_response = client.delete('/api/projects/woop/skills/test_create_skill', follow_redirects=True)
    assert delete_response.status_code == HTTPStatus.NOT_FOUND
