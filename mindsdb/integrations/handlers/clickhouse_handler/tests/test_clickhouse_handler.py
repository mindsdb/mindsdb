import unittest
from mindsdb.integrations.handlers.clickhouse_handler.clickhouse_handler import ClickHouseHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class PostgresHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            "host": "localhost",
            "port": "9000",
            "user": "root",
            "password": "pass",
            "database": "test_data"
        }
        cls.handler = ClickHouseHandler('test_clickhouse_handler', connection_data)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_show_dbs(self):
        result = self.handler.native_query("SHOW DATABASES;")
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_2_wrong_native_query_returns_error(self):
        result = self.handler.native_query("SHOW DATABASE1S;")
        assert result.type is RESPONSE_TYPE.ERROR

    def test_3_select_query(self):
        query = 'SELECT * FROM hdi'
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_get_tables(self):
        tbls = self.handler.get_tables()
        assert tbls.type is not RESPONSE_TYPE.ERROR

    def test_5_describe_table(self):
        described = self.handler.get_columns("hdi")
        print('described', described)
        assert described.type is RESPONSE_TYPE.TABLE
