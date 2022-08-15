import unittest
from mindsdb.integrations.handlers.bigquery_handler.bigquery_handler import BigQueryHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE


class BigQueryHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {            
            "project_id": "tough-future-332513",
            "service_account_keys": "/home/bigq/tough-future-332513-6699d9b39601.json"
        }
        cls.handler = BigQueryHandler('test_bigquery_handler', connection_data)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_list_tables(self):        
        datasets = self.handler.get_tables('Rentals')
        assert datasets.type is not RESPONSE_TYPE.ERROR

    def test_2_list_table_columns(self):        
        datasets = self.handler.get_columns('Rentals', 'home_rentals')
        assert datasets.type is not RESPONSE_TYPE.ERROR