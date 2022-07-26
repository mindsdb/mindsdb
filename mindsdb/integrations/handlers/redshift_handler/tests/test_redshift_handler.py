import unittest
from mindsdb.integrations.handlers.redshift_handler.redshift_handler import RedshiftHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class RedshiftHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": "examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com",
            "port": 5439,
            "database": "dev",
            "user": "awsuser",
            "password": "my_password"
        }
        cls.handler = RedshiftHandler('test_redshift_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM sales"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_4_get_columns(self):
        columns = self.handler.get_columns('sales')
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()