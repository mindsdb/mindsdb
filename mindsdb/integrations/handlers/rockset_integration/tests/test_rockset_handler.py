import unittest

from mindsdb.integrations.handlers.rockset_integration.rockset_integration import RocksetIntegration
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class RocksetHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host":'127.0.0.1',
                "port": 8080,
                "user": "",
                "password": "",
                "database": "database",
                "schema": "PUBLIC",
                "protocol": "https",
                "api_key": "api_key",
                "api_region": "https://api.use1a1.rockset.com"
            }
        }
        cls.handler = RocksetIntegration('test_rockset_handler', cls.kwargs)

    def test_check_connection(self):
        assert self.handler.check_connection()

    def test_connect(self):
        assert self.handler.connect()

    def test_create_table(self):
        query = "CREATE TABLE test_table (id INT, name VARCHAR(255))"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_insert_into_table(self):
        query = "INSERT INTO test_table (id, name) VALUES (1, 'test')"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_native_query(self):
        query = "SELECT * FROM test_table"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_drop_table(self):
        query = "DROP TABLE test_table"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_get_tables(self):
        tables = self.handler.get_tables()
        assert table is RESPONSE_TYPE.TABLE

    def test_get_columns(self):
        columns = self.handler.get_columns('test_table')
        query = "DROP Table IF EXISTS test_table;"
        result = self.handler.query(query)
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_get_primary_keys(self):
        primary_keys = self.handler.get_primary_keys('_id')
        query = "DROP Table IF EXISTS _id"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR

if __name__ == '__main__':
    unittest.main()
