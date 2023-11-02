import os
import unittest

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.iceberg_handler.iceberg_handler import IcebergHandler


class IcebergHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            'name': os.environ["ICEBERG_NAME"],
            'namespace': os.environ["ICEBERG_NAMESPACE"],
            'user': os.environ["ICEBERG_USER"],
            'password': os.environ["ICEBERG_PASSWORD"],
            'database': os.environ["ICEBERG_DATABASE"],
            'table': os.environ["ICEBERG_TABLE"],
        }
        cls.handler = IcebergHandler('test_iceberg_handler', connection_data)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_show_tables(self):
        res = self.handler.query('SHOW TABLES;')
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_create_table(self):
        res = self.handler.query(
            'CREATE TABLE test_table(id INTEGER, name VARCHAR(255));'
        )
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_4_insert_into_table(self):
        res = self.handler.query("INSERT INTO test_table VALUES (1, 'test')")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_5_select(self):
        res = self.handler.query('SELECT * FROM test_table;')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_6_describe_table(self):
        res = self.handler.get_columns('test_table')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_7_get_tables(self):
        res = self.handler.get_tables()
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_8_drop_table(self):
        res = self.handler.query('DROP TABLE IF EXISTS test_table;')
        assert res.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()
