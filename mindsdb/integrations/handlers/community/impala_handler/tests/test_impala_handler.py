import unittest
from mindsdb.integrations.handlers.impala_handler.impala_handler import ImpalaHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class ImpalaHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            'user': '<UID>',
            'password': '<P455w0rd>',
            'host': '127.0.0.1',
            'port': 21050,
            'database': 'temp'

        }
        cls.handler = ImpalaHandler('test_impala_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_connect(self):
        assert self.handler.connect()

    def test_2_create_table(self):
        query = "CREATE Table Car(Name Varchar, Price Integer);"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_3_insert(self):
        query = "INSERT INTO Car ('Tata SUV', 860000)"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_4_native_query_select(self):
        query = "SELECT * FROM Car;"
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is RESPONSE_TYPE.TABLE

    def test_6_get_columns(self):
        columns = self.handler.get_columns('Car')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
