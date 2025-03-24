from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from salesforce_api.exceptions import AuthenticationError

from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.handlers.salesforce_handler.salesforce_handler import SalesforceHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse
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


if __name__ == '__main__':
    unittest.main()