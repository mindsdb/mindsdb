import unittest
from mindsdb.integrations.handlers.firebird_handler.firebird_handler import FirebirdHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class FirebirdHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "localhost",
                "database": r"C:\Users\minura\Documents\mindsdb\test.fdb",
                "user": "sysdba",
                "password": "password"
            }
        }
        cls.handler = FirebirdHandler('test_firebird_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM test_tbl"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_4_get_columns(self):
        columns = self.handler.get_columns('test_tbl')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
