import unittest
from mindsdb.integrations.handlers.cockroach_handler.cockroach_handler import CockroachHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class CockroachHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "localhost",
                "port": "5432",
                "user": "mindsdb",
                "password": "mindsdb",
                "database": "postgres"
            }
        }
        cls.handler = CockroachHandler('test_cockroach_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_describe_table(self):
        described = self.handler.describe_table("test_mdb")
        assert described['type'] is not RESPONSE_TYPE.ERROR

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables['type'] is not RESPONSE_TYPE.ERROR

    def test_4_select_query(self):
        query = "SELECT * FROM data.test_mdb WHERE 'id'='1'"
        result = self.handler.query(query)
        assert result['type'] is RESPONSE_TYPE.TABLE
