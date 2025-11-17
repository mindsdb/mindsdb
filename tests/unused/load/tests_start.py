from locust import between, HttpUser
from tests.load.test_postgresql import PostgreSQLConnectionBehavior
from tests.utils.config import get_value_from_json_env_var

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class DBConnectionUser(HttpUser):
    tasks = [PostgreSQLConnectionBehavior]
    wait_time = between(5, 15)
    config = get_value_from_json_env_var("INTEGRATIONS_CONFIG", "mindsdb_cloud")
    host = config['host']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            response = self.client.post('/cloud/login', json={
                'email': self.config['user'],
                'password': self.config['password']
            })
            response.raise_for_status()
        except Exception as e:
            logger.error(f'Logging to MindsDB failed: {e}')
