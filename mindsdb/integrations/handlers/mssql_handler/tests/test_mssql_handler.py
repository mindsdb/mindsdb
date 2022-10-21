import unittest
from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class SqlServerHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
           "connection_data": {
                "host": "localhost",
                "port": "1433",
                "user": "sa",
                "password": "",
                "database": "master"
           }
        }
        cls.handler = SqlServerHandler('test_sqlserver_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables['type'] is not RESPONSE_TYPE.ERROR

    def test_4_select_query(self):
        query = "SELECT * FROM test_data.home_rentals"
        result = self.handler.native_query(query)
        assert result['type'] is RESPONSE_TYPE.TABLE