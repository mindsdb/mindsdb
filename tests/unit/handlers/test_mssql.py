import unittest
import pymssql
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict
from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler


class TestMSSQLHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        host='127.0.0.1',
        port=1433,
        user='example_user',
        password='example_pass',
        database='example_db',
    )

    def setUp(self):
        self.patcher = patch('pymssql.connect')
        self.mock_connect = self.patcher.start()
        self.handler = SqlServerHandler('mssql', connection_data={'connection_data': self.dummy_connection_data})

    def tearDown(self):
        self.patcher.stop()

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that pymssql.connect is called exactly once.
        """
        
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()