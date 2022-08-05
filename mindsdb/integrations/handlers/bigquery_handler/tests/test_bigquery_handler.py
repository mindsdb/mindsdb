import unittest
from mindsdb.integrations.handlers.bigquery_handler.bigquery_handler import BigQueryHandler


class PostgresHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {            
            "project_id": "tough-future-332513",
            "service_account_keys": "/home/bigq/tough-future-332513.json"
        }
        cls.handler = BigQueryHandler('test_bigquery_handler', connection_data)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

