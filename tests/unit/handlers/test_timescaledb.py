from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

import psycopg
from psycopg.pq import ExecStatus

from base_handler_test import BaseDatabaseHandlerTest, MockCursorContextManager
from mindsdb.integrations.handlers.timescaledb_handler.timescaledb_handler import TimeScaleDBHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response
)


class TestTimescaleHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='127.0.0.1',
            port=5432,
            user='example_user',
            schema='public',
            password='example_pass',
            database='example_db'
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return psycopg.Error("Connection Failed")

    @property
    def get_tables_query(self):
        return """
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

    @property
    def get_columns_query(self):
        return f"""
            SELECT
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
            FROM
                information_schema.columns
            WHERE
                table_name = '{self.mock_table}'
            AND
                table_schema = current_schema()
        """

    def create_handler(self):
        return TimeScaleDBHandler('timescaledb', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('psycopg.connect')

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query using a mock cursor,
        returns a Response object, and correctly handles the ExecStatus scenario
        """
        # TODO: Can this be handled via the base class? The use of ExecStatus is specific to Postgres.
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

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


if __name__ == '__main__':
    unittest.main()
