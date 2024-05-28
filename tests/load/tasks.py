from locust import between, HttpUser, TaskSet, task
from tests.load.test_postgresql import PostgreSQLConnectionBehavior
from tests.utils.config import get_value_from_json_env_var
from mindsdb.utilities import log

logger = log.getLogger(__name__)

class PostgreSQLTaskSet(TaskSet):
    @task
    def postgresql_connection(self):
        PostgreSQLConnectionBehavior().run(self)

class DBConnectionUser(HttpUser):
    tasks = [PostgreSQLTaskSet]
    wait_time = between(5, 15)
    
    config = get_value_from_json_env_var("INTEGRATIONS_CONFIG", "mindsdb_cloud")
    
    if 'host' not in config or 'user' not in config or 'password' not in config:
        raise ValueError("Configuration must include 'host', 'user', and 'password'")
    
    host = config['host']

    def on_start(self):
        try:
            response = self.client.post('/cloud/login', json={
                'email': self.config['user'],
                'password': self.config['password']
            })
            response.raise_for_status()
        except Exception as e:
            logger.error(f'Logging to MindsDB failed: {e}')
            self.environment.runner.quit()  # Stops the Locust test

    def on_stop(self):
        # Any cleanup logic if needed
        pass
