import unittest
from collections import OrderedDict
from unittest.mock import patch, MagicMock
from google.api_core.exceptions import BadRequest

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.handlers.bigquery_handler.bigquery_handler import BigQueryHandler


class TestBigQueryHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        project_id='tough-future-332513',
        dataset='example_ds',
        service_account_keys='example_keys',
    )

    def setUp(self):
        self.patcher_get_oauth2_credentials = patch('mindsdb.integrations.utilities.handlers.auth_utilities.GoogleServiceAccountOAuth2Manager.get_oauth2_credentials')
        self.patcher_client = patch('mindsdb.integrations.handlers.bigquery_handler.bigquery_handler.Client')
        self.mock_get_oauth2_credentials = self.patcher_get_oauth2_credentials.start()
        self.mock_connect = self.patcher_client.start()
        self.handler = BigQueryHandler('bigquery', connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher_get_oauth2_credentials.stop()
        self.patcher_client.stop()

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that google.cloud.bigquery.Client is called exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """
        Ensures that the connect method correctly handles a connection failure by raising a google.api_core.exceptions.BadRequest and sets is_connected to False.
        """
        self.mock_connect.side_effect = BadRequest("Connection Failed")

        with self.assertRaises(BadRequest):
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
        Tests the `native_query` method to ensure it executes a SQL query using the mock query object and returns a Response object.
        """
        mock_conn = MagicMock()
        self.handler.connect = MagicMock(return_value=mock_conn)

        mock_query = MagicMock()
        mock_query.to_dataframe.return_value = None
        mock_conn.query.return_value = mock_query

        query_str = "SELECT * FROM table"

        with patch('mindsdb.integrations.handlers.bigquery_handler.bigquery_handler.QueryJobConfig') as mock_query_job_config:
            mock_query_job_config_instance = mock_query_job_config.return_value
            data = self.handler.native_query(query_str)
            mock_conn.query.assert_called_once_with(query_str, job_config=mock_query_job_config_instance)
            assert isinstance(data, Response)
            self.assertFalse(data.error_code)

    def test_get_tables(self):
        """
        Checks if the `get_tables` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        self.handler.native_query = MagicMock()

        self.handler.get_tables()

        expected_query = f"""
            SELECT table_name, table_schema, table_type
            FROM `{self.dummy_connection_data['project_id']}.{self.dummy_connection_data['dataset']}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type IN ('BASE TABLE', 'VIEW')
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
            SELECT column_name AS Field, data_type as Type
            FROM `{self.dummy_connection_data['project_id']}.{self.dummy_connection_data['dataset']}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
        """

        self.handler.native_query.assert_called_once_with(expected_query)


if __name__ == '__main__':
    unittest.main()
