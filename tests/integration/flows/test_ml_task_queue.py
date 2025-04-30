import json
import os

import pytest
from walrus import Database

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.utilities.ml_task_queue.const import TASKS_STREAM_NAME

from tests.utils.http_test_helpers import HTTPHelperMixin
from tests.utils.config import HTTP_API_ROOT

REDIS_HOST = os.environ.get("INTERNAL_URL", "").replace("mindsdb", "redis-master")


@pytest.mark.skipif("localhost" in HTTP_API_ROOT or "127.0.0.1" in HTTP_API_ROOT, reason="Requires redis")
class TestMLTaskQueue(HTTPHelperMixin):

    def test_redis_connection(self):
        db = Database(protocol=3, host=REDIS_HOST)
        db.ping()

    def test_create_model(self, train_finetune_lock):
        """ 1. create db connection
            2. create test dataset
            3. start to train model in 'async' mode: check status
            4. start to train model in 'sync' mode: check status
            5. await model 2 is finished
            6. 2 messages in redis stream
        """

        db_details = {
            "type": "postgres",
            "connection_data": {
                "type": "postgres",
                "host": "samples.mindsdb.com",
                "port": "5432",
                "user": "demo_user",
                "password": "demo_password",
                "database": "demo"
            }
        }

        self.sql_via_http("DROP MODEL IF EXISTS p_test_queue_async;", RESPONSE_TYPE.OK)
        self.sql_via_http("DROP MODEL IF EXISTS p_test_queue_sync;", RESPONSE_TYPE.OK)

        query = f"""
            CREATE DATABASE IF NOT EXISTS test_demo_queue
            WITH ENGINE = 'postgres',
            PARAMETERS = {json.dumps(db_details['connection_data'])};
        """
        self.sql_via_http(query, RESPONSE_TYPE.OK)

        with train_finetune_lock.acquire(timeout=600):
            query = """
                create predictor p_test_queue_async
                from test_demo_queue (select sqft, location, rental_price from demo_data.home_rentals limit 30)
                predict rental_price;
            """
            response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
            status = response['data'][0][response['column_names'].index('STATUS')]
            assert status in ('generating', 'training')

            query = """
                create predictor p_test_queue_sync
                from test_demo_queue (select sqft, location, rental_price from demo_data.home_rentals limit 30)
                predict rental_price
                USING join_learn_process=true;
            """
            response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
            status = response['data'][0][response['column_names'].index('STATUS')]
            assert status == 'complete'

            status = self.await_model('p_test_queue_async')
            assert status == 'complete'

        db = Database(protocol=3, host=REDIS_HOST)
        assert TASKS_STREAM_NAME in db.keys()

        assert db.type(TASKS_STREAM_NAME) == b'stream'
        xlen = db.xlen(TASKS_STREAM_NAME)
        if xlen != 0:
            lol = db.xrange(TASKS_STREAM_NAME)
            assert False, "Caught non-zero length ml queue: " + str(lol)
        assert db.xlen(TASKS_STREAM_NAME) == 0

    def test_predict(self):
        """ make predict queries to both trained models
        """

        query = """
            SELECT rental_price,
                rental_price_explain
            FROM mindsdb.p_test_queue_async
            WHERE b = 10;
        """
        response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(response['data']) == 1
        assert len(response['data'][0]) == 2

        query = """
            SELECT rental_price,
                rental_price_explain
            FROM test_demo_queue.demo_data.home_rentals
            JOIN mindsdb.p_test_queue_async
            LIMIT 3;
        """
        response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(response['data']) == 3
        assert len(response['data'][0]) == 2

        db = Database(protocol=3, host=REDIS_HOST)
        assert db.xlen(TASKS_STREAM_NAME) == 0

    def test_finetune(self, train_finetune_lock):
        """ check that finetune is working
        """

        with train_finetune_lock.acquire(timeout=600):
            query = """
                FINETUNE p_test_queue_sync
                FROM test_demo_queue (SELECT * FROM demo_data.home_rentals LIMIT 10)
                USING join_learn_process=true;
            """
            response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
            status = response['data'][0][response['column_names'].index('STATUS')]
            assert status == 'complete'

            query = """
                FINETUNE p_test_queue_async
                FROM test_demo_queue (SELECT * FROM demo_data.home_rentals LIMIT 10);
            """
            response = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
            status = response['data'][0][response['column_names'].index('STATUS')]
            # FINETUNE in this case may be very fast, so add 'complete' to check
            assert status in ('generating', 'training', 'complete')

            status = self.await_model('p_test_queue_async', version_number=2)
            assert status == 'complete'

        db = Database(protocol=3, host=REDIS_HOST)
        assert db.xlen(TASKS_STREAM_NAME) == 0
