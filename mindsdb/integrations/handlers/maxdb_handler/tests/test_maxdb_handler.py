import unittest

from mindsdb.integrations.handlers.maxdb_handler.maxdb_handler import MaxDBHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class SurrealdbHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "127.0.0.1",
                "port": "7210",
                "user": "MAXDB",
                "password": "password",
                "database": "MAXDB",
                "jdbc_location": "/path/to/jdbc/sapdbc.jar"
            }
        }
        cls.handler = MaxDBHandler('test_maxdb_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_create_table(self):
        res = self.handler.native_query("CREATE TABLE TEST_TABLE (id INT PRIMARY KEY,name VARCHAR(50))")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_2_insert(self):
        res = self.handler.native_query("INSERT INTO TEST_TABLE (id, name) VALUES (1, 'MARSID')")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_select_query(self):
        query = "SELECT * FROM TEST_TABLE"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_get_columns(self):
        columns = self.handler.get_columns('TEST_TABLE')
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_5_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_6_drop_table(self):
        res = self.handler.native_query("DROP TABLE TEST_TABLE")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_7_disconnect(self):
        assert self.handler.disconnect() is None


if __name__ == '__main__':
    unittest.main()
