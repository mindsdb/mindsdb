from http import HTTPStatus

import datetime as dt


def test_jobs_flow(client):
    # --- create ---

    date_format = '%Y-%m-%d %H:%M:%S'
    start_at = (dt.datetime.now() + dt.timedelta(days=1)).strftime(date_format)
    end_at = (dt.datetime.now() + dt.timedelta(days=2)).strftime(date_format)
    job = {
        'name': 'test_job',
        'query': 'select 1',
        'if_query': 'select 2',
        'start_at': start_at,
        'end_at': end_at,
        'schedule_str': 'every hour',
    }
    request = {
        'job': job
    }

    response = client.post('/api/projects/mindsdb/jobs', json=request)
    assert response.status_code == HTTPStatus.OK
    created_job = response.json

    for field in ['name', 'query', 'if_query', 'schedule_str']:
        assert created_job[field] == job[field]

    # dates, created date could have milliseconds, compare as substring
    assert job['start_at'] in created_job['start_at']
    assert job['end_at'] in created_job['end_at']

    # --- get created ---

    response = client.get('/api/projects/mindsdb/jobs/test_job')
    assert response.status_code == HTTPStatus.OK
    job_resp = response.json
    assert job_resp['query'] == job['query']

    # --- get history ---

    response = client.get('/api/projects/mindsdb/jobs/test_job/history')
    assert response.status_code == HTTPStatus.OK
    # no executions
    assert len(response.get_json()) == 0

    # --- get list ---

    response = client.get('/api/projects/mindsdb/jobs')
    assert response.status_code == HTTPStatus.OK
    assert len(response.get_json()) == 1

    # check first job
    job_resp = response.json[0]
    assert job_resp['name'] == 'test_job'

    # --- delete job ---

    response = client.delete('/api/projects/mindsdb/jobs/test_job')
    assert response.status_code == HTTPStatus.NO_CONTENT

    # got deleted
    response = client.get('/api/projects/mindsdb/jobs/test_job')
    assert response.status_code == HTTPStatus.NOT_FOUND
