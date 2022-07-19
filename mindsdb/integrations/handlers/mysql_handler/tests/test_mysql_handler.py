import unittest

from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class MySQLHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": "localhost",
            "port": "3306",
            "user": "root",
            "password": "root",
            "database": "test",
            "ssl": False
        }
        cls.handler = MySQLHandler('test_mysql_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.check_connection()

    def test_1_native_query_show_dbs(self):
        dbs = self.handler.native_query("SHOW DATABASES;")
        assert dbs['type'] is not RESPONSE_TYPE.ERROR

    def test_2_get_tables(self):
        tbls = self.handler.get_tables()
        assert tbls['type'] is not RESPONSE_TYPE.ERROR

    def test_3_get_views(self):
        views = self.handler.get_views()
        assert views['type'] is not RESPONSE_TYPE.ERROR

    def test_5_drop_table(self):
        res = self.handler.native_query("DROP TABLE IF EXISTS test_mdb")
        assert res['type'] is not RESPONSE_TYPE.ERROR 

    def test_4_create_table(self):
        res = self.handler.native_query("CREATE TABLE IF NOT EXISTS test_mdb (test_col INT)")
        assert res['type'] is not RESPONSE_TYPE.ERROR 

    def test_6_describe_table(self):
        described = self.handler.get_columns("test_mdb")
        assert described['type'] is RESPONSE_TYPE.TABLE

    def test_7_select_query(self):
        query = "SELECT * FROM test_mdb WHERE 'id'='a'"
        result = self.handler.query(query)
        assert result['type'] is RESPONSE_TYPE.TABLE
