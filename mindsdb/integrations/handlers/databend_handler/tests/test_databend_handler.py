import unittest
from mindsdb.integrations.handlers.databend_handler.databend_handler import DatabendHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class DatabendHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            "protocol": "https",
            "host": "some-url.aws-us-east-2.default.databend.com",
            "port": 443,
            "user": "root",
            "password": "password",
            "database": "test_db"
        }
        cls.handler = DatabendHandler('test_databend_handler', connection_data)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_show_dbs(self):
        result = self.handler.native_query("SHOW DATABASES;")
        assert result.type is not RESPONSE_TYPE.ERROR

    # def test_2_wrong_native_query_returns_error(self):
    #     result = self.handler.native_query("SHOW DATABASE1S;")
    #     assert result.type is RESPONSE_TYPE.ERROR

    def test_3_select_query(self):
        query = 'SELECT * FROM price'
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_get_tables(self):
        tbls = self.handler.get_tables()
        assert tbls.type is not RESPONSE_TYPE.ERROR

    def test_5_describe_table(self):
        described = self.handler.get_columns("price")
        print('described', described)
        assert described.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()