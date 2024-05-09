import os
import datetime as dt
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
    with TemporaryDirectory(prefix='jobs_test_') as temp_dir:
        db_path = 'sqlite:///' + os.path.join(temp_dir, 'mindsdb.sqlite3.db')
        # Need to change env variable for migrate module, since it calls db.init().
        os.environ['MINDSDB_DB_CON'] = db_path
        db.init()
        migrate.migrate_to_head()
        app = initialize_app(Config(), True)
        yield app

    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app):
    return app.test_client()


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
    assert '200' in response.status
    created_job = response.json

    for field in ['name', 'query', 'if_query', 'schedule_str']:
        assert created_job[field] == job[field]

    # dates, created date could have milliseconds, compare as substring
    assert job['start_at'] in created_job['start_at']
    assert job['end_at'] in created_job['end_at']

    # --- get created ---

    response = client.get('/api/projects/mindsdb/jobs/test_job')
    assert '200' in response.status
    job_resp = response.json
    assert job_resp['query'] == job['query']

    # --- get history ---

    response = client.get('/api/projects/mindsdb/jobs/test_job/history')
    assert '200' in response.status
    # no executions
    assert len(response.get_json()) == 0

    # --- get list ---

    response = client.get('/api/projects/mindsdb/jobs')
    assert '200' in response.status
    assert len(response.get_json()) == 1

    # check first job
    job_resp = response.json[0]
    assert job_resp['name'] == 'test_job'

    # --- delete job ---

    response = client.delete('/api/projects/mindsdb/jobs/test_job')
    assert '204' in response.status

    # got deleted
    response = client.get('/api/projects/mindsdb/jobs/test_job')
    assert '404' in response.status
