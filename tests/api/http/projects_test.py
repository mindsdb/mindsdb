# Import necessary libraries and modules
import os
import pytest
from tempfile import TemporaryDirectory

# Import modules from MindsDB
from mindsdb.api.http.initialize import initialize_app
from mindsdb.migrations import migrate
from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config

# Define a pytest fixture for the 'app' object
@pytest.fixture(scope="session", autouse=True)
def app():
    # Store the original 'MINDSDB_DB_CON' environment variable if it exists
    old_minds_db_con = ''
    if 'MINDSDB_DB_CON' in os.environ:
        old_minds_db_con = os.environ['MINDSDB_DB_CON']
    
    # Create a temporary directory for the database file
    with TemporaryDirectory(prefix='projects_test_') as temp_dir:
        # Create a database connection URL based on the temporary directory
        db_path = 'sqlite:///' + os.path.join(temp_dir, 'mindsdb.sqlite3.db')
        
        # Set the 'MINDSDB_DB_CON' environment variable to the new database URL
        os.environ['MINDSDB_DB_CON'] = db_path
        
        # Initialize the database
        db.init()
        
        # Migrate the database to the latest version
        migrate.migrate_to_head()
        
        # Initialize the MindsDB application with the given configuration
        app = initialize_app(Config(), True, False)

        yield app  # Provide the 'app' object to test functions

    # Restore the original 'MINDSDB_DB_CON' environment variable if it existed
    os.environ['MINDSDB_DB_CON'] = old_minds_db_con

# Define a pytest fixture for the 'client' object
@pytest.fixture()
def client(app):
    return app.test_client()

# Define a test function to get all projects
def test_get_projects(client):
    response = client.get('/api/projects', follow_redirects=True)
    all_projects = response.get_json()
    
    # Assert that there is at least one project (the default project)
    assert len(all_projects) == 1
    assert all_projects[0]['name'] == 'mindsdb'

# Define a test function to get a specific project (the default project)
def test_get_project(client):
    response = client.get('/api/projects/mindsdb', follow_redirects=True)
    default_project = response.get_json()
    assert default_project['name'] == 'mindsdb'

# Define a test function to test the scenario where a project is not found
def test_get_project_not_found(client):
    response = client.get('/api/projects/zoop', follow_redirects=True)
    assert '404' in response.status
