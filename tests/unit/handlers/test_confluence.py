from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from mindsdb_sql_parser.ast import BinaryOperation, Constant, Identifier, Select, Star

from base_handler_test import BaseHandlerTestSetup, BaseAPIResourceTestSetup
from mindsdb.integrations.handlers.confluence_handler.confluence_handler import ConfluenceHandler
from mindsdb.integrations.handlers.salesforce_handler.salesforce_tables import create_table_class
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)


class TestConfluenceHandler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            api_base='https://demo.atlassian.net/',
            username='demo@example.com',
            password='demo_password',
        )

    def create_handler(self):
        return ConfluenceHandler('confluence', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('requests.Session')

    def test_connect(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        The `connect` method for this handler does not check the validity of the connection; it succeeds even with incorrect credentials.
        The `check_connection` method handles the connection status.
        """
        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)

    def test_check_connection_success(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        mock_request = MagicMock()
        mock_request.return_value = MagicMock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: dict(
                results=[],
                _links=dict(next=None)
            ),
        )
        self.mock_connect.return_value = MagicMock(request=mock_request)

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/spaces",
            params={
                "description-format": "view",
                "limit": 1
            },
            json=None
        )

    def test_check_connection_failure(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a failed connection.
        """
        mock_request = MagicMock()
        mock_request.return_value = MagicMock(
            status_code=401,
            raise_for_status=lambda: None,
        )
        self.mock_connect.return_value = MagicMock(request=mock_request)
        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/spaces",
            params={
                "description-format": "view",
                "limit": 1
            },
            json=None
        )
        

if __name__ == '__main__':
    unittest.main()