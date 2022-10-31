from msilib.schema import tables
import unittest

from matplotlib import table
from sqlalchemy import column
from mindsdb.integrations.handlers.rockset_integration.rockset_integration import RocksetIntegration
from mindsdb.integrations.libs.response import RESPONSE_TYPE

class RocksetHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "password",
                "database": "database"
            }
        }
        cls.handler = RocksetIntegration('test_rockset_handler', **cls.kwargs)

    def test_connection_args(self):
        self.assertEqual(self.handler.connection_args, {
            'user': {
                'type': ARG_TYPE.STR,
                'description': 'The user name used to authenticate with the OceanBase server.'
            },
            'password': {
                'type': ARG_TYPE.STR,
                'description': 'The password to authenticate the user with the OceanBase server.'
            },
            'database': {
                'type': ARG_TYPE.STR,
                'description': 'The database name to use when connecting with the OceanBase server.'
            },
            'host': {
                'type': ARG_TYPE.STR,
                'description': 'The host name or IP address of the OceanBase server. '
            },
            'port': {
                'type': ARG_TYPE.INT,
                'description': 'The TCP/IP port of the OceanBase server. Must be an integer.'
            }
        })

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

    def test_disconnect(self):
        assert self.handler.disconnect()

    def test_get_tables(self):
        tables = self.handler.get_tables()
        assert table is RESPONSE_TYPE.TABLE

    def test_get_columns(self):
        columns = self.handler.get_columns('_event_time')
        query = "DROP Table IF EXISTS _event_time;"
        result = self.handler.query(query)
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_get_primary_keys(self):
        primary_keys = self.handler.get_primary_keys('_id')
        query = "DROP Table IF EXISTS _id"
        result = self.handler.query(query)
        assert result.type is not RESPONSE_TYPE.ERROR
