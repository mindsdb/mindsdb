import unittest
from mindsdb.integrations.handlers.vertica_handler.vertica_handler import VerticaHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class VerticaHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": '127.0.0.1',
                "port": 5433,
                "user": 'dbadmin',
                "password": '',
                "database": 'VMart',
                "schema_name": 'public'
            }

        }
        cls.handler = VerticaHandler('test_vertica_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_connect(self):
        assert self.handler.connect()

    def test_2_create_table(self):
        query = "CREATE Table TEST(id Number(1),Name Varchar(33))"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_3_insert(self):
        query = "INSERT INTO TEST (1,'lOVe yOU)"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_4_native_query_select(self):
        query = "SELECT * FROM TEST;"
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.TABLE

    def test_6_get_columns(self):
        columns = self.handler.get_columns('TEMP')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
