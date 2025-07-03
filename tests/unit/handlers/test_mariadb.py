import unittest
from unittest.mock import patch, MagicMock
from collections import OrderedDict

from mysql.connector import Error as MySQLError

from base_handler_test import BaseDatabaseHandlerTest, MockCursorContextManager
from mindsdb.integrations.handlers.mariadb_handler.mariadb_handler import MariaDBHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response


class TestMariaDBHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='127.0.0.1',
            port=3307,
            user='example_user',
            password='example_pass',
            database='example_db',
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return MySQLError("Connection Failed")

    @property
    def get_tables_query(self):
        return """
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

    @property
    def get_columns_query(self):
        return f"""
            select
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION,
                COLUMN_DEFAULT,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_OCTET_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                CHARACTER_SET_NAME,
                COLLATION_NAME
            from
                information_schema.columns
            where
                table_name = '{self.mock_table}';
        """

    def create_handler(self):
        return MariaDBHandler('mariadb', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mysql.connector.connect')

    def test_native_query(self):
        """Test that native_query returns a Response object with no error
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager(
            data=[{'id': 1}],
            description=[('id', 3, None, None, None, None, 1, 0, 45)]
        )

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        query_str = f"SELECT * FROM {self.mock_table}"
        data = self.handler.native_query(query_str)

        self.assertIsInstance(data, Response)
        self.assertFalse(data.error_code)


if __name__ == '__main__':
    unittest.main()
