import unittest
from mindsdb.integrations.handlers.lindorm_handler.lindorm_handler import LindormHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class LindormHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "url": 'http://127.0.0.1:8765',
            "autocommit": True
        }
        cls.handler = LindormHandler('test_phoenix_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM USERS"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns('USERS')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
