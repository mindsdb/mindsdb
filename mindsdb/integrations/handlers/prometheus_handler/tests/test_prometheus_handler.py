import unittest
from mindsdb.integrations.handlers.prometheus_handler.prometheus_handler import PrometheusHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class PrometheusHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": "http://127.0.0.1:9090",
        }
        cls.handler = PrometheusHandler('test_prometheus_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_get_columns(self):
        columns = self.handler.get_columns('baseballStats')
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_3_native_query_select(self):
        query = "prometheus_http_requests_total"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_native_query_select(self):
        query = "http_requests_total_fake"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()
