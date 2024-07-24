import unittest
import elasticsearch
from unittest.mock import patch, MagicMock
from collections import OrderedDict

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler import ElasticsearchHandler


class TestElasticsearchHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        hosts='http://localhost:9200',
        user='example_user',
        password='example_pass'
    )

    def setUp(self):
        self.patcher = patch('mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.Elasticsearch')
        self.mock_connect = self.patcher.start()
        self.handler = ElasticsearchHandler('elasticsearch', connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that elasticsearch.Elasticsearch is called exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """
        Ensures that the connect method correctly handles a connection failure by raising a elasticsearch.exceptions.AuthenticationException and setting is_connected to False.
        There can be other exceptions that can be raised by the connect method such as elasticsearch.exceptions.ConnectionError, etc.
        """
        self.mock_connect.side_effect = elasticsearch.exceptions.AuthenticationException("Connection Failed", 403)

        with self.assertRaises(elasticsearch.exceptions.AuthenticationException):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_check_connection(self):
        """
        Verifies that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        connected = self.handler.check_connection()
        self.assertTrue(connected)
        assert isinstance(connected, StatusResponse)
        self.assertFalse(connected.error_message)

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query and returns a Response object.
        """
        mock_conn = MagicMock()
        mock_conn.sql.query = MagicMock(
            return_value={
                'rows': [[1, 2.0]],
                'columns': [
                    {'name': 'column1', 'type': 'integer'},
                    {'name': 'column2', 'type': 'float'},
                ],
            }
        )

        self.handler.connect = MagicMock(return_value=mock_conn)

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)

        mock_conn.sql.query.assert_called_once_with(body={'query': query_str})
        assert isinstance(data, Response)
        self.assertFalse(data.error_code)

    def test_get_tables(self):
        """
        Tests the `get_tables` method to confirm it correctly calls `native_query` with the appropriate SQL commands.
        """
        self.handler.native_query = MagicMock()

        self.handler.get_tables()

        expected_query = """
            SHOW TABLES
        """

        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_columns(self):
        """
        Checks if the `get_columns` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        self.handler.native_query = MagicMock()

        table_name = 'mock_table'
        self.handler.get_columns(table_name)

        expected_query = f"""
            DESCRIBE {table_name}
        """

        self.handler.native_query.assert_called_once_with(expected_query)


if __name__ == '__main__':
    unittest.main()
