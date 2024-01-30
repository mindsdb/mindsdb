import unittest
import psycopg
from psycopg.pq import ExecStatus
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)


class CursorContextManager(Mock):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestPostgresHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        host='127.0.0.1',
        port=5432,
        user='example_user',
        schema='public',
        password='example_pass',
        database='example_db',
        sslmode='prefer'
    )

    def setUp(self):
        self.patcher = patch('psycopg.connect')
        self.mock_connect = self.patcher.start()
        self.handler = PostgresHandler('psql', connection_data={'connection_data': self.dummy_connection_data})

    def tearDown(self):
        self.patcher.stop()

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that psycopg.connect is called exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """
        Ensures that the connect method correctly handles a connection failure
        by raising a psycopg.Error and sets is_connected to False.
        """
        self.mock_connect.side_effect = psycopg.Error("Connection Failed")

        with self.assertRaises(psycopg.Error):
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
        Tests the `native_query` method to ensure it executes a SQL query using a mock cursor,
        returns a Response object, and correctly handles the ExecStatus scenario
        """
        mock_conn = MagicMock()
        mock_cursor = CursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.execute.return_value = None

        mock_pgresult = MagicMock()
        mock_pgresult.status = ExecStatus.COMMAND_OK
        mock_cursor.pgresult = mock_pgresult

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)
        mock_cursor.execute.assert_called_once_with(query_str)
        assert isinstance(data, Response)
        self.assertFalse(data.error_code)

    def test_get_columns(self):
        """
        Checks if the `get_columns` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        self.handler.native_query = MagicMock()

        table_name = 'mock_table'
        self.handler.get_columns(table_name)

        expected_query = f"""
            SELECT
                column_name as "Field",
                data_type as "Type"
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
        """
        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_tables(self):
        """
        Tests the `get_tables` method to confirm it correctly calls `native_query` with the appropriate SQL command.
        """
        self.handler.native_query = MagicMock()
        self.handler.get_tables()

        expected_query = """
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema NOT IN ('information_schema', 'pg_catalog')
                and table_type in ('BASE TABLE', 'VIEW')
                and table_schema = current_schema()
        """

        self.handler.native_query.assert_called_once_with(expected_query)


if __name__ == '__main__':
    unittest.main()
