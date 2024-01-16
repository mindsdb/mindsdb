import unittest
from mindsdb.integrations.handlers.access_handler.access_handler import AccessHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class AccessHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "db_file": 'C:\\Users\\minurap\\Documents\\example_db.accdb',
        }
        cls.handler = AccessHandler('test_access_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM example_tbl"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns('example_tbl')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
