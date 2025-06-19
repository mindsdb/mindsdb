from http import HTTPStatus


def test_get_projects(client):
    response = client.get('/api/projects', follow_redirects=True)
    all_projects = response.get_json()
    # Should contain default project.
    assert len(all_projects) == 1
    assert all_projects[0]['name'] == 'mindsdb'


def test_get_project(client):
    response = client.get('/api/projects/mindsdb', follow_redirects=True)
    default_project = response.get_json()
    assert default_project['name'] == 'mindsdb'


def test_get_project_not_found(client):
    response = client.get('/api/projects/zoop', follow_redirects=True)
    assert response.status_code == HTTPStatus.NOT_FOUND
