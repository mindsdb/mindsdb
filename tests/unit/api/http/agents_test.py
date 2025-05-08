from http import HTTPStatus
from unittest.mock import patch

import pandas as pd

from tests.api.http.conftest import create_demo_db, create_dummy_ml


def test_prepare(client):
    create_demo_db(client)
    create_dummy_ml(client)
    # Create model to use in all tests.
    create_query = '''
        CREATE MODEL mindsdb.test_model
        FROM example_db (SELECT location as answer, sqft FROM demo_data.home_rentals limit 10)
        PREDICT answer
        USING engine = 'dummy_ml', join_learn_process = true
    '''
    train_data = {
        'query': create_query
    }
    response = client.post('/api/projects/mindsdb/models', json=train_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED

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

    create_response = client.post('/api/projects/mindsdb/skills', json=create_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.CREATED

    create_request = {
        'skill': {
            'name': 'test_skill_2',
            'type': 'Knowledge Base',
            'params': {
                'k1': 'v1'
            }
        }
    }

    create_response = client.post('/api/projects/mindsdb/skills', json=create_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.CREATED


def test_post_agent(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'provider': 'mindsdb',
            'skills': ['test_skill']
        }
    }

    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.CREATED

    created_agent = create_response.get_json()

    expected_agent = {
        'name': 'test_post_agent',
        'model_name': 'test_model',
        'provider': 'mindsdb',
        'params': {
            'k1': 'v1'
        },
        'skills': created_agent['skills'],
        'skills_extra_parameters': created_agent['skills_extra_parameters'],
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
    assert create_response.status_code == HTTPStatus.BAD_REQUEST


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
    assert create_response.status_code == HTTPStatus.BAD_REQUEST


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
    assert create_response.status_code == HTTPStatus.BAD_REQUEST


def test_post_agent_project_not_found(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent_no_model_name',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'provider': 'mindsdb',
            'skills': ['test_skill']
        }
    }
    create_response = client.post('/api/projects/womp/agents', json=create_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.NOT_FOUND


def test_post_agent_model_not_found(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent_model_not_found',
            'model_name': 'not_the_model_youre_looking_for',
            'params': {
                'k1': 'v1'
            },
            'provider': 'mindsdb',
            'skills': ['test_skill']
        }
    }
    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.NOT_FOUND


def test_post_agent_skill_not_found(client):
    create_request = {
        'agent': {
            'name': 'test_post_agent_skill_not_found',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'provider': 'mindsdb',
            'skills': ['overpowered_skill']
        }
    }
    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.NOT_FOUND


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
    assert get_response.status_code == HTTPStatus.OK
    all_agents = get_response.get_json()
    assert len(all_agents) > 0


def test_get_agents_project_not_found(client):
    get_response = client.get('/api/projects/bloop/agents', follow_redirects=True)
    assert get_response.status_code == HTTPStatus.NOT_FOUND


def test_get_agent(client):
    create_request = {
        'agent': {
            'name': 'test_get_agent',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1'
            },
            'provider': 'mindsdb',
            'skills': ['test_skill']
        }
    }

    client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)

    get_response = client.get('/api/projects/mindsdb/agents/test_get_agent', follow_redirects=True)
    assert get_response.status_code == HTTPStatus.OK
    agent = get_response.get_json()

    expected_agent = {
        'name': 'test_get_agent',
        'model_name': 'test_model',
        'params': {
            'k1': 'v1'
        },
        'skills': agent['skills'],
        'skills_extra_parameters': agent['skills_extra_parameters'],
        'id': agent['id'],
        'provider': 'mindsdb',
        'project_id': agent['project_id'],
        'created_at': agent['created_at'],
        'updated_at': agent['updated_at']
    }

    assert agent == expected_agent


def test_get_agent_project_not_found(client):
    get_response = client.get('/api/projects/bloop/agents/test_get_agent', follow_redirects=True)
    assert get_response.status_code == HTTPStatus.NOT_FOUND

# At the moment creation via PUT is not allowed
# def test_put_agent_create(client):
#     create_request = {
#         'agent': {
#             'name': 'test_put_agent_create',
#             'model_name': 'test_model',
#             'params': {
#                 'k1': 'v1'
#             },
#             'provider': 'mindsdb',
#             'skills': ['test_skill']
#         }
#     }

#     put_response = client.put('/api/projects/mindsdb/agents/test_put_agent_create', json=create_request, follow_redirects=True)
#     assert put_response.status_code == HTTPStatus.CREATED

#     created_agent = put_response.get_json()

#     expected_agent = {
#         'name': 'test_put_agent_create',
#         'model_name': 'test_model',
#         'params': {
#             'k1': 'v1'
#         },
#         'provider': 'mindsdb',
#         'skills': created_agent['skills'],
#         'skills_extra_parameters': created_agent['skills_extra_parameters'],
#         'id': created_agent['id'],
#         'project_id': created_agent['project_id'],
#         'created_at': created_agent['created_at'],
#         'updated_at': created_agent['updated_at']
#     }

#     assert created_agent == expected_agent


def test_put_agent_update(client):
    create_request = {
        'agent': {
            'name': 'test_put_agent_update',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1',
                'k2': 'v2'
            },
            'provider': 'mindsdb',
            'skills': ['test_skill']
        }
    }

    response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED

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
        'provider': 'mindsdb',
        'skills': updated_agent['skills'],
        'skills_extra_parameters': updated_agent['skills_extra_parameters'],
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
    assert response.status_code == HTTPStatus.BAD_REQUEST


# At the moment creation via PUT is not allowed
# def test_put_agent_model_not_found(client):
#     create_request = {
#         'agent': {
#             'name': 'test_put_agent_model_not_found',
#             'model_name': 'oopsy_daisy',
#             'params': {
#                 'k1': 'v1',
#                 'k2': 'v2'
#             },
#             'provider': 'mindsdb',
#             'skills': ['test_skill']
#         }
#     }

#     response = client.put('/api/projects/mindsdb/agents/test_put_agent_model_not_found', json=create_request, follow_redirects=True)
#     assert '404' in response.status


def test_delete_agent(client):
    create_request = {
        'agent': {
            'name': 'test_delete_agent',
            'model_name': 'test_model',
            'params': {
                'k1': 'v1',
                'k2': 'v2'
            },
            'provider': 'mindsdb',
            'skills': ['test_skill']
        }
    }

    client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)

    delete_response = client.delete('/api/projects/mindsdb/agents/test_delete_agent', follow_redirects=True)
    assert delete_response.status_code == HTTPStatus.NO_CONTENT

    get_response = client.get('/api/projects/mindsdb/agents/test_delete_agent', follow_redirects=True)
    assert get_response.status_code == HTTPStatus.NOT_FOUND


def test_delete_agent_project_not_found(client):
    delete_response = client.delete('/api/projects/doop/agents/test_post_agent', follow_redirects=True)
    assert delete_response.status_code == HTTPStatus.NOT_FOUND


def test_delete_agent_not_found(client):
    delete_response = client.delete('/api/projects/mindsdb/agents/test_delete_agent_not_found', follow_redirects=True)
    assert delete_response.status_code == HTTPStatus.NOT_FOUND


def test_agent_completions(client):
    create_request = {
        'agent': {
            'name': 'test_agent',
            'model_name': 'test_model',
            'provider': 'mindsdb',
            'params': {'prompt_template': 'Test message!',
                       'user_column': 'content'},
        }
    }

    create_response = client.post('/api/projects/mindsdb/agents', json=create_request, follow_redirects=True)
    assert create_response.status_code == HTTPStatus.CREATED

    completions_request = {
        'messages': [
            {'role': 'user', 'content': 'Test message!'}
        ]
    }

    with patch('mindsdb.interfaces.agents.langchain_agent.LangchainAgent') as agent_mock:
        agent_mock_instance = agent_mock.return_value
        agent_mock_instance.get_completion.return_value = pd.DataFrame([{'answer': 'beepboop', 'trace_id': '---'}])
        completions_response = client.post(
            '/api/projects/mindsdb/agents/test_agent/completions',
            json=completions_request,
            follow_redirects=True
        )

        assert completions_response.status_code == HTTPStatus.OK
        message_json = completions_response.get_json()['message']
        assert message_json['content'] == 'beepboop'


def test_agent_completions_project_not_found(client):
    completions_request = {
        'messages': [
            {'role': 'user', 'content': 'Test message!'}
        ]
    }
    completions_response = client.post('/api/projects/bloop/agents/test_agent/completions', json=completions_request, follow_redirects=True)
    assert completions_response.status_code == HTTPStatus.NOT_FOUND


def test_agent_completions_bad_request(client):
    completions_request = {
        'massagez': [
            {'role': 'user', 'content': 'Test message!'}
        ]
    }
    completions_response = client.post('/api/projects/mindsdb/agents/test_agent/completions', json=completions_request, follow_redirects=True)
    assert completions_response.status_code == HTTPStatus.BAD_REQUEST


def test_agent_completions_agent_not_found(client):
    completions_request = {
        'messages': [
            {'role': 'user', 'content': 'Test message!'}
        ]
    }
    completions_response = client.post('/api/projects/mindsdb/agents/zoopy_agent/completions', json=completions_request, follow_redirects=True)
    assert completions_response.status_code == HTTPStatus.NOT_FOUND
