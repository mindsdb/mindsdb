import unittest
from mindsdb.integrations.handlers.ignite_handler.ignite_handler import IgniteHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class IgniteHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": "127.0.0.1",
            "port": 10800
        }
        cls.handler = IgniteHandler('test_ignite_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM City"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_4_get_columns(self):
        columns = self.handler.get_columns('City')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
