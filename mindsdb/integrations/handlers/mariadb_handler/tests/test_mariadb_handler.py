import unittest
import pandas as pd
from mindsdb.integrations.handlers.mariadb_handler.mariadb_handler import MariaDBHandler


class MariaDBHandlerTest(unittest.TestCase):
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
        cls.handler = MariaDBHandler('test_mysql_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        assert self.handler.check_connection()

    def test_2_native_query_show_dbs(self):
        dbs = self.handler.native_query("SHOW DATABASES;")
        assert isinstance(dbs, list)

    def test_3_get_tables(self):
        tbls = self.handler.get_tables()
        assert isinstance(tbls, list)

    def test_4_get_views(self):
        views = self.handler.get_views()
        assert isinstance(views, list)

    def test_5_create_table(self):
        try:
            self.handler.native_query("CREATE TABLE test_mdb (test_col INT)")
        except Exception:
            pass

    def test_6_describe_table(self):
        described = self.handler.describe_table("dt_test")
        assert isinstance(described, list)

    def test_7_select_query(self):
        query = "SELECT * FROM dt_test WHERE 'id'='a'"
        result = self.handler.select_query(query)
