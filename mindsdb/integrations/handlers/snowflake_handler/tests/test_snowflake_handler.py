from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import SnowflakeHandler
import unittest
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb_sql import parse_sql


class SnowflakeHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": "us-east-1.snowflakecomputing.com",
            "port": "443",
            "user": "",
            "password": "",
            "database": "DEMO_DB",
            "warehouse": "COMPUTE_WH",
            "account": "us-east-1.aws",
            "schema": "PUBLIC",
            "protocol": "https"
        }
        cls.handler = SnowflakeHandler('test_snowflake_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tbls = self.handler.get_tables()
        assert tbls.type is not RESPONSE_TYPE.ERROR

    def test_2_get_columns(self):
        tbls = self.handler.get_columns('home_rentals')
        assert tbls.type is not RESPONSE_TYPE.ERROR

    def test_3_select_native(self):
        query = "SELECT * FROM home_rentals WHERE sqft=484"
        ast = parse_sql(query)
        result = self.handler.query(ast)
        assert result.type is RESPONSE_TYPE.TABLE
