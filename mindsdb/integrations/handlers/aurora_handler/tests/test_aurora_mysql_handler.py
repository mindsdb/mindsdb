import unittest
from mindsdb.integrations.handlers.aurora_handler.aurora_handler import AuroraHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class AuroraMySQLHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "",
                "port": 3306,
                "user": "",
                "password": "",
                "database": "",
                "db_engine": "mysql"
            }
        }
        cls.handler = AuroraHandler('test_aurora_mysql_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = ""
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns('')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
