import json

import pytest
from walrus import Database

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.utilities.ml_task_queue.const import TASKS_STREAM_NAME

from .http_test_helpers import HTTPHelperMixin


# used by mindsdb_app fixture in conftest
OVERRIDE_CONFIG = {
    'ml_task_queue': {
        'type': 'redis'
    },
    'tasks': {'disable': True},
    'jobs': {'disable': True}
}

ML_TASK_QUEUE_CONSUMER = True

API_LIST = ["http"]


@pytest.mark.usefixtures('redis', 'postgres_db', 'mindsdb_app')
class TestMLTaskQueue(HTTPHelperMixin):
    def test_redis_connection(self):
        db = Database(protocol=3)
        db.ping()

    def test_create_model(self):
        """ 1. create db connection
            2. create test dataset
            3. start to train model in 'async' mode: check status
            4. start to train model in 'sync' mode: check status
            5. await model 2 is finished
            6. 2 messages in redis stream
        """

        query = f"""
            CREATE DATABASE pg
            WITH ENGINE = 'postgres',
            PARAMETERS = {json.dumps(self.postgres_db['connection_data'])};
        """
        self.sql_via_http(query, RESPONSE_TYPE.OK)

        query = """
            SELECT * FROM pg (
                CREATE TABLE test_data AS
                SELECT
                    generate_series as id,
                    random()*100 as var_a,
                    random()*100 as var_b
                FROM generate_series(1,50)
            )
        """
        self.sql_via_http(query, RESPONSE_TYPE.TABLE)

        query = """
            CREATE MODEL
            mindsdb.test_model_async
            FROM pg
            (SELECT * FROM test_data)
            PREDICT var_a;
        """
        response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        status = response['data'][0][response['column_names'].index('STATUS')]
        assert status in ('generating', 'training')

        query = """
            CREATE MODEL
            mindsdb.test_model_sync
            FROM pg
            (SELECT * FROM test_data)
            PREDICT var_a
            USING join_learn_process=true;
        """
        response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        status = response['data'][0][response['column_names'].index('STATUS')]
        assert status == 'complete'

        status = self.await_model('test_model_async')
        assert status == 'complete'

        db = Database(protocol=3)
        assert TASKS_STREAM_NAME in db.keys()
        assert db.type(TASKS_STREAM_NAME) == b'stream'
        assert db.xlen(TASKS_STREAM_NAME) == 0

    def test_predict(self):
        """ make predict queries to both trained models
        """

        query = """
            SELECT var_a,
                var_a_explain
            FROM mindsdb.test_model_async
            WHERE b = 10;
        """
        response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(response['data']) == 1
        assert len(response['data'][0]) == 2

        query = """
            SELECT var_a,
                var_a_explain
            FROM pg.test_data
            JOIN mindsdb.test_model_sync
            LIMIT 3;
        """
        response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(response['data']) == 3
        assert len(response['data'][0]) == 2

        db = Database(protocol=3)
        assert db.xlen(TASKS_STREAM_NAME) == 0

    def test_finetune(self):
        """ check that finetune is work
        """

        query = """
            FINETUNE test_model_async
            FROM pg (SELECT * FROM test_data LIMIT 10);
        """
        response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        status = response['data'][0][response['column_names'].index('STATUS')]
        # FINETUNE in this case may be very fast, so add 'complete' to check
        assert status in ('generating', 'training', 'complete')

        query = """
            FINETUNE test_model_sync
            FROM pg (SELECT * FROM test_data LIMIT 10)
            USING join_learn_process=true;
        """
        response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        status = response['data'][0][response['column_names'].index('STATUS')]
        assert status == 'complete'

        status = self.await_model('test_model_async', version_number=2)
        assert status == 'complete'

        db = Database(protocol=3)
        assert db.xlen(TASKS_STREAM_NAME) == 0
