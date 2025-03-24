from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from salesforce_api.exceptions import AuthenticationError
from mindsdb_sql_parser.ast import Select, Identifier, Star
import pandas as pd

from base_handler_test import BaseHandlerTestSetup, BaseAPIResourceTestSetup
from mindsdb.integrations.handlers.salesforce_handler.salesforce_handler import SalesforceHandler
from mindsdb.integrations.handlers.salesforce_handler.salesforce_tables import create_table_class
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)


class TestSalesforceHandler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            username='demo@example.com',
            password='demo_password',
            client_id='3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY',
            client_secret='5A52C1A1E21DF9012IODC9ISNXXAADDA9',
        )

    def create_handler(self):
        return SalesforceHandler('salesforce', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('salesforce_api.Salesforce')

    def test_connect(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that salesforce_api.Salesforce is instantiated exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_check_connection_success(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_check_connection_failure(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a failed connection.
        """
        self.mock_connect.side_effect = AuthenticationError('Invalid credentials')
        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    def test_get_tables(self):
        """
        Test that the `get_tables` method returns a list of tables mapped from the Salesforce API.
        """
        mock_tables = ['Account', 'Contact']
        self.mock_connect.return_value = MagicMock(
            sobjects=MagicMock(
                describe=lambda: {'sobjects': [{'name': table} for table in mock_tables]}
            )
        )
        self.handler.connect()
        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), len(mock_tables))
        self.assertEqual(list(df['table_name']), [table.lower() for table in mock_tables])

    def test_get_columns(self):
        """
        Test that the `get_columns` method returns a list of columns for a given table.
        """
        mock_columns = ['Id', 'Name', 'Email']
        mock_table = 'Contact'
        self.mock_connect.return_value = MagicMock(
            sobjects=MagicMock(
                describe=lambda: {'sobjects': [{'name': mock_table}]},
                Contact=MagicMock(
                    describe=lambda: {'fields': [{'name': column} for column in mock_columns]}
                )
            )
        )
        self.handler.connect()
        response = self.handler.get_columns(mock_table.lower())

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), len(mock_columns))
        self.assertEqual(list(df['Field']), mock_columns)


class TestSalesforceAnyTable(BaseAPIResourceTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            username='demo@example.com',
            password='demo_password',
            client_id='3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY',
            client_secret='5A52C1A1E21DF9012IODC9ISNXXAADDA9',
        )
    
    @property
    def table_name(self):
        return 'Contact'

    def create_handler(self):
        return SalesforceHandler('salesforce', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('salesforce_api.Salesforce')
    
    def create_resource(self):
        return create_table_class(self.table_name)(self.handler)


if __name__ == '__main__':
    unittest.main()