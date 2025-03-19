from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast import Select, Identifier, Star, Constant
import pandas as pd

from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_handler import MSTeamsHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException


class TestMSTeamsHandler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            client_id='12345678-90ab-cdef-1234-567890abcdef',
            client_secret='a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6',
            tenant_id='abcdef12-3456-7890-abcd-ef1234567890'
        )
    
    def create_handler(self):
        mock_handler_storage = MagicMock()
        mock_handler_storage.file_get.side_effect = FileNotFoundError

        return MSTeamsHandler(
            'teams',
            connection_data=self.dummy_connection_data,
            handler_storage=mock_handler_storage
        )
    
    def create_patcher(self):
        return patch('msal.ConfidentialClientApplication')

    def test_connect_without_code_returns_redirect_url(self):
        """
        Test if `connect` method successfully returns a redirect URL when the authentication code is not provided.
        """
        mock_msal = MagicMock()
        mock_msal.get_accounts.return_value = []
        mock_auth_url = 'https://mock.auth.url'
        mock_msal.get_authorization_request_url.return_value = mock_auth_url

        self.mock_connect.return_value = mock_msal

        with self.assertRaises(AuthException, msg=f'Authorisation required. Please follow the url: {mock_auth_url}'):
            self.handler.connect()

        self.assertFalse(self.handler.is_connected)


if __name__ == '__main__':
    unittest.main()
