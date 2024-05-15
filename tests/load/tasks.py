from locust import SequentialTaskSet, task, events
from tests.utils.query_generator import QueryGenerator as query
from tests.utils.config import get_value_from_json_env_var, generate_random_db_name

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class BaseDBConnectionBehavior(SequentialTaskSet):
    def on_start(self):
        """This method is called once for each user when they start."""
        self.query_generator = query()
        self.random_db_name = generate_random_db_name(f"{self.db_type}_datasource")
        self.create_new_datasource()

    def __post_query(self, query):
        try:
            response = self.client.post('/api/sql/query', json={'query': query})
            response.raise_for_status()
            assert response.json()['type'] != 'error'
            return response
        except Exception as e:
            logger.error(f'Error running {query}: {e}')
            events.request.fire(request_type="POST", name="/api/sql/query", response_time=0, response_length=0, exception=e)
            self.interrupt(reschedule=True)

    def create_new_datasource(self):
        """This method creates a new data source."""
        db_config = get_value_from_json_env_var("INTEGRATIONS_CONFIG", self.db_type)
        query = self.query_generator.create_database_query(
            self.random_db_name,
            self.db_type,
            db_config
        )
        self.__post_query(query)

    @task
    def select_integration_query(self):
        """This task performs a SELECT query from integration."""
        query = f'SELECT * FROM {self.random_db_name}.{self.table_name} LIMIT 10'
        self.__post_query(query)

    @task
    def run_native_query(self):
        """This task runs a native DB select query."""
        for n_query in self.native_queries:
            query = f'SELECT * FROM {self.random_db_name}( {n_query})'
            self.__post_query(query)
