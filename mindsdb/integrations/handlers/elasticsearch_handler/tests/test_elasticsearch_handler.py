import unittest
from mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler import (
    ElasticsearchHandler,
)
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class ElasticsearchHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {"hosts": "localhost:9200"}
        cls.handler = ElasticsearchHandler("test_elasticsearch_handler", cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT customer_first_name, customer_full_name FROM kibana_sample_data_ecommerce"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns("kibana_sample_data_ecommerce")
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()
