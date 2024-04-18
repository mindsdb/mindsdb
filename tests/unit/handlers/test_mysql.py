import unittest
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mysql.connector import Error as MySQLError
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response
)


class CursorContextManager(Mock):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestMySQLHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        host='127.0.0.1',
        port=3306,
        user='example_user',
        password='example_pass',
        database='example_db',
        url='mysql://example_user:example_pass@localhost:3306/example_db'
    )

    def setUp(self):
        self.patcher = patch('mysql.connector.connect')
        self.mock_connect = self.patcher.start()
        self.handler = MySQLHandler('mysql', connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_connect_success(self):
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        self.mock_connect.side_effect = MySQLError("Connection Failed")

        with self.assertRaises(MySQLError):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_check_connection(self):
        self.mock_connect.return_value = MagicMock()
        connected = self.handler.check_connection()
        self.assertTrue(connected)

    def test_native_query(self):
        mock_conn = MagicMock()
        mock_cursor = CursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)

        assert isinstance(data, Response)
        self.assertFalse(data.error_code)

    def test_get_columns(self):
        self.handler.native_query = MagicMock()

        table_name = 'mock_table'
        self.handler.get_columns(table_name)

        expected_query = f"DESCRIBE `{table_name}`;"
        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_tables(self):
        self.handler.native_query = MagicMock()
        self.handler.get_tables()

        expected_query = """
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                TABLE_TYPE AS table_type
            FROM
                information_schema.TABLES
            WHERE
                TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                AND TABLE_SCHEMA = DATABASE()
            ORDER BY 2
            ;
        """

        self.handler.native_query.assert_called_once_with(expected_query)


if __name__ == '__main__':
    unittest.main()
