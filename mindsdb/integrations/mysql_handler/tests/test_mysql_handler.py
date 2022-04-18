import unittest
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.mysql_handler.mysql_handler.mysql_handler import MySQLHandler


class MySQLHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config()
        cls.kwargs = {
            "host": "localhost",
            "port": "3306",
            "user": "root",
            "password": "root",
            "database": "test",
            "ssl": False
        }
        cls.handler = MySQLHandler('test_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_status(self):
        assert self.handler.check_status()

    def test_2_native_query_show_dbs(self):
        dbs = self.handler.native_query("SHOW DATABASES;")
        assert isinstance(dbs, list)

    def test_3_get_tables(self):
        tbls = self.handler.get_tables()
        assert isinstance(tbls, list)

    def test_4_get_views(self):
        views = self.handler.get_views()
        assert isinstance(views, list)

    def test_5_drop_table(self):
        try:
            result = self.handler.native_query("DROP TABLE test_mdb")
        except:
            self.handler.native_query("CREATE TABLE test_mdb (test_col INT)")
            self.test_5_drop_table_native()

    def test_6_create_table(self):
        try:
            self.handler.native_query("CREATE TABLE test_mdb (test_col INT)")
        except Exception:
            pass

    def test_7_describe_table(self):
        described = self.handler.describe_table("test_mdb")
        assert isinstance(described, list)

    def test_8_select_query(self):
        query = "SELECT * FROM test_mdb WHERE 'id'='a'"
        result = self.handler.select_query(query)

    def test_8_select_into(self):
        try:
            result = self.handler.native_query("DROP TABLE test_mdb2")
        except:
            pass
        try:
            self.handler.native_query("CREATE TABLE test_mdb2 (test_col INT)")
        except Exception:
            pass

        result = self.handler.select_into('test_mdb2', pd.DataFrame.from_dict({'test_col': [1]}))

        tbls = self.handler.get_tables()
        assert 'test_mdb2' in [item['Tables_in_test'] for item in tbls]

    def test_9_join(self):
        self.handler.native_query("INSERT INTO test_mdb(test_col) VALUES (1)")
        self.handler.native_query("INSERT INTO test_mdb2(test_col) VALUES (1)")

        into_table = 'test_join_into_mysql'
        query = f"SELECT test_col FROM test_mdb JOIN test_mdb2"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        result = self.handler.join(parsed, self.handler, into=into_table)
        assert len(result) > 0

        q = f"SELECT * FROM {into_table}"
        qp = self.handler.parser(q, dialect='mysql')
        assert len(self.handler.select_query(query)) > 0