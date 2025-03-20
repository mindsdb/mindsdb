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
            tenant_id='abcdef12-3456-7890-abcd-ef1234567890',
            code='1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef',  # Not passed by the user, but by the front-end.
        )

    @property
    def dummy_connection_data_without_code(self):
        return OrderedDict(
            client_id='12345678-90ab-cdef-1234-567890abcdef',
            client_secret='a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6',
            tenant_id='abcdef12-3456-7890-abcd-ef1234567890'
        )
    
    def create_handler(self):
        mock_handler_storage = MagicMock()
        mock_handler_storage.file_get.side_effect = FileNotFoundError
        mock_handler_storage.file_set.return_value = None

        return MSTeamsHandler(
            'teams',
            connection_data=self.dummy_connection_data,
            handler_storage=mock_handler_storage
        )
    
    def create_handler_without_code_without_cache(self):
        mock_handler_storage = MagicMock()
        mock_handler_storage.file_get.side_effect = FileNotFoundError

        return MSTeamsHandler(
            'teams',
            connection_data=self.dummy_connection_data_without_code,
            handler_storage=mock_handler_storage
        )
    
    def create_handler_without_code_with_cache(self):
        mock_handler_storage = MagicMock()
        mock_handler_storage.file_get.side_effect = b'mock_file_content'

        return MSTeamsHandler(
            'teams',
            connection_data=self.dummy_connection_data_without_code,
            handler_storage=mock_handler_storage
        )
    
    def create_patcher(self):
        return patch('msal.ConfidentialClientApplication')

    def test_connect_without_code_returns_redirect_url(self):
        """
        Test if `connect` method successfully returns a redirect URL when the authentication code is not provided.
        """
        self.handler = self.create_handler_without_code_without_cache()

        mock_msal = MagicMock()
        mock_msal.get_accounts.return_value = []
        mock_auth_url = 'https://mock.auth.url'
        mock_msal.get_authorization_request_url.return_value = mock_auth_url

        self.mock_connect.return_value = mock_msal

        with self.assertRaises(AuthException, msg=f'Authorisation required. Please follow the url: {mock_auth_url}'):
            self.handler.connect()

        self.assertFalse(self.handler.is_connected)

        # TODO: Assert connect() is only called once.

    @patch('msal.SerializableTokenCache')
    def test_connect_with_valid_code_returns_access_token(self, mock_token_cache):
        """"
        Test if `connect` method successfully returns an access token when the authentication code is provided and is valid.
        """
        mock_msal = MagicMock()
        mock_msal.get_accounts.return_value = []

        mock_msal.acquire_token_by_authorization_code.return_value = {
            "access_token": "mock_access_token"
        }

        self.mock_connect.return_value = mock_msal

        mock_token_cache.has_state_changed = True

        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        # TODO: Assert connect() is called only once.

        # TODO: Asserts _register_table() is called for each table.

    @patch('msal.SerializableTokenCache')
    def test_connect_with_cache_returns_access_code(self, mock_token_cache):
        """"
        Test if `connect` method successfully returns an access token when some content is available in the cache.
        """
        self.handler = self.create_handler_without_code_with_cache()

        mock_msal = MagicMock()
        mock_msal.get_accounts.return_value = [
            {
                "mock_key": "mock_value"
            }
        ]

        mock_msal.acquire_token_silent.return_value = {
            "access_token": "mock_access_token"
        }

        self.mock_connect.return_value = mock_msal

        mock_token_cache.has_state_changed = True

        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        # TODO: Assert connect() is called only once.

        # TODO: Asserts _register_table() is called for each table.


if __name__ == '__main__':
    unittest.main()
