import unittest
from mindsdb.integrations.handlers.sqreamdb_handler.sqreamdb_handler import SQreamDBHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class SQreamDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "127.0.0.1",
                "port": "5000",
                "user": "sqream",
                "password": "sqream",
                "database": "master"
            }
        }
        cls.handler = SQreamDBHandler('test_sqreamdb_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_drop_table(self):
        res = self.handler.query("DROP TABLE IF EXISTS LOVE")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_2_create_table(self):
        res = self.handler.query("CREATE TABLE IF NOT EXISTS LOVE (LOVER varchar(20))")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_4_select_query(self):
        query = "SELECT * FROM AUTHORS"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_check_connection(self):
        self.handler.check_connection()


if __name__ == '__main__':
    unittest.main()
