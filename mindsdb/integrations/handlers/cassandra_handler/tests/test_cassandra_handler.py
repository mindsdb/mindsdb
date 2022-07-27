import unittest
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.cassandra_handler.cassandra_handler import CassandraHandler


class CassandraHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "127.0.0.1",
                "port": "9043",
                "user": "cassandra",
                "password": "",
                "keyspace": "test_data",
                "protocol_version": 4
            }
        }
        cls.handler = CassandraHandler('test_cassandra_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.check_connection()

    def test_1_native_query_show_keyspaces(self):
        dbs = self.handler.native_query("DESC KEYSPACES;")
        assert dbs.type is not RESPONSE_TYPE.ERROR

    def test_2_get_tables(self):
        tbls = self.handler.get_tables()
        assert tbls.type is not RESPONSE_TYPE.ERROR

    def test_3_describe_table(self):
        described = self.handler.get_columns("home_rentals")
        assert described.type is RESPONSE_TYPE.TABLE

    def test_4_select_query(self):
        query = "SELECT * FROM home_rentals WHERE 'id'='3712'"
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE
