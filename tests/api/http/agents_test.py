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

        # Create skill to use in all tests.
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

        yield app
    os.environ['MINDSDB_DB_CON'] = old_minds_db_con


@pytest.fixture()
def client(app):
    return app.test_client()