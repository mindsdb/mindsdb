import unittest
from mindsdb.integrations.handlers.orioledb_handler.orioledb_handler import OrioleDBHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE


class OrioleDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "1234",
                "database": "postgres",
            }
        }
        cls.handler = OrioleDBHandler('test_orioledb_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_connect(self):
        assert self.handler.connect()

    def test_2_create_table(self):
        query = "CREATE Table IF NOT EXISTS Lover(name varchar(101));"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_3_insert(self):
        query = "INSERT INTO LOVER VALUES('Shiv Shakti');"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_4_native_query_select(self):
        query = "SELECT * FROM LOVER;"
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is RESPONSE_TYPE.TABLE

    def test_6_get_columns(self):
        columns = self.handler.get_columns('LOVER')

        query = "DROP Table IF  EXISTS Lover;"
        self.handler.query(query)
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
