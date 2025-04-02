from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

from base_handler_test import BaseHandlerTestSetup, BaseAPIResourceTestSetup
from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_handler import MSTeamsHandler
from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_tables import (
    TeamsTable,
    ChannelsTable,
    ChannelMessagesTable,
    ChatsTable,
    ChatMessagesTable
)
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


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

    def test_connect_with_invalid_code_raises_error(self):
        """"
        Test if `connect` method raises an AuthException when the authentication code is invalid.
        """
        mock_msal = MagicMock()
        mock_msal.get_accounts.return_value = []

        mock_msal.acquire_token_by_authorization_code.return_value = {
            "error": "invalid_grant",
            "error_description": "AADSTS70000: The provided authorization code is invalid or has expired."
        }

        self.mock_connect.return_value = mock_msal

        with self.assertRaises(AuthException, msg='Error getting access token: AADSTS70000: The provided authorization code is invalid or has expired.'):
            self.handler.connect()

        self.assertFalse(self.handler.is_connected)

    @patch('msal.SerializableTokenCache')
    def test_connect_with_cache_returns_access_code(self, mock_token_cache):
        """"
        Test if `connect` method successfully returns an access token when valid information is found in the cache.
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

    @patch('requests.get')
    def test_check_connection_with_successful_connection(self, mock_get):
        """"
        Test if `check_connection` method successfully returns a StatusResponse object with success=True and no error message when the connection check is successful.
        """
        mock_msal = MagicMock()
        mock_msal.get_accounts.return_value = []

        mock_msal.acquire_token_by_authorization_code.return_value = {
            "access_token": "mock_access_token"
        }

        self.mock_connect.return_value = mock_msal

        mock_response = MagicMock(
            status_code=200
        )
        mock_get.return_value = mock_response

        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertTrue(response.success)
        self.assertFalse(response.error_message)

    @patch('requests.get')
    def test_check_connection_with_failed_connection(self, mock_get):
        """"
        Test if `check_connection` method successfully returns a StatusResponse object with success=False and an error message when the connection check fails.
        """
        mock_msal = MagicMock()
        mock_msal.get_accounts.return_value = []

        mock_msal.acquire_token_by_authorization_code.return_value = {
            "access_token": "mock_access_token"
        }

        self.mock_connect.return_value = mock_msal

        mock_response = MagicMock(
            status_code=400
        )
        mock_get.return_value = mock_response

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)


class MSTeamsResourceTestSetup(BaseAPIResourceTestSetup):
    @property
    def dummy_connection_data(self):
        return OrderedDict(
            client_id='12345678-90ab-cdef-1234-567890abcdef',
            client_secret='a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6',
            tenant_id='abcdef12-3456-7890-abcd-ef1234567890',
            code='1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef',  # Not passed by the user, but by the front-end.
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

    def create_patcher(self):
        return patch('msal.ConfidentialClientApplication')

    def setUp(self):
        super().setUp()
        mock_msal = MagicMock()
        mock_msal.get_accounts.return_value = []

        mock_msal.acquire_token_by_authorization_code.return_value = {
            "access_token": "mock_access_token"
        }

        self.mock_connect.return_value = mock_msal

    def generate_mock_data(self, columns=None):
        if columns is None:
            columns = self.resource.get_columns()
        return {column: f"mock_{column}" for column in columns}


class TestMSTeamsTeamsTable(MSTeamsResourceTestSetup, unittest.TestCase):
    def create_resource(self):
        return TeamsTable(self.handler)

    @patch('requests.get')
    def test_list(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for all teams.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_team = self.generate_mock_data()
        mock_response.json.return_value = {
            "value": [
                mock_team
            ]
        }
        mock_get.return_value = mock_response

        response = self.resource.list()

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_team]))


class TestMSTeamsChannelsTable(MSTeamsResourceTestSetup, unittest.TestCase):
    def create_resource(self):
        return ChannelsTable(self.handler)

    @patch('requests.get')
    def test_list_all(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for all channels in all teams.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_channel = self.generate_mock_data()
        mock_response.json.return_value = {
            "value": [
                mock_channel
            ]
        }
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_channel]))

    @patch('requests.get')
    def test_list_with_team_id(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for all channels in a team.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_channel = self.generate_mock_data()
        mock_response.json.return_value = {
            "value": [
                mock_channel
            ]
        }
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='teamId',
                    op=FilterOperator.EQUAL,
                    value="mock_team_id"
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_channel]))

    @patch('requests.get')
    def test_list_with_channel_id(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for a specific channel in a team.
        """
        mock_response_1 = MagicMock(
            status_code=200
        )
        mock_response_2 = MagicMock(
            status_code=200
        )

        mock_team = self.generate_mock_data(
            columns=TeamsTable(self.handler).get_columns()
        )
        mock_channel = self.generate_mock_data()
        mock_response_1.json.return_value = {
            "value": [
                mock_team
            ]
        }
        mock_response_2.json.return_value = {
            "value": [
                mock_channel
            ]
        }

        mock_get.side_effect = [mock_response_1, mock_response_2]

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='id',
                    op=FilterOperator.EQUAL,
                    value="mock_id"
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_channel]))

    @patch('requests.get')
    def test_list_with_team_id_and_channel_id(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for a specific channel in a specific team.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_channel = self.generate_mock_data()
        mock_response.json.return_value = mock_channel
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='teamId',
                    op=FilterOperator.EQUAL,
                    value="mock_team_id"
                ),
                FilterCondition(
                    column='id',
                    op=FilterOperator.EQUAL,
                    value="mock_id"
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_channel]))


class TestMSTeamsChannelMessagesTable(MSTeamsResourceTestSetup, unittest.TestCase):
    def create_resource(self):
        return ChannelMessagesTable(self.handler)

    def test_list_without_team_id_and_channel_id_raises_error(self):
        """"
        Test if `list` method raises a ValueError when teamId and channelId are not provided.
        """
        with self.assertRaises(ValueError, msg="The 'channelIdentity_teamId' and 'channelIdentity_channelId' columns are required."):
            self.resource.list(conditions=[])

    @patch('requests.get')
    def test_list_with_team_id_and_channel_id(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for all messages in a specific channel in a specific team.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_message = self.generate_mock_data()
        mock_response.json.return_value = {
            "value": [
                mock_message
            ]
        }
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='channelIdentity_teamId',
                    op=FilterOperator.EQUAL,
                    value="mock_team_id"
                ),
                FilterCondition(
                    column='channelIdentity_channelId',
                    op=FilterOperator.EQUAL,
                    value="mock_channel_id"
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_message]))

    @patch('requests.get')
    def test_list_with_team_id_channel_id_and_message_id(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for a specific message in a specific channel in a specific team.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_message = self.generate_mock_data()
        mock_response.json.return_value = mock_message
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='channelIdentity_teamId',
                    op=FilterOperator.EQUAL,
                    value="mock_team_id"
                ),
                FilterCondition(
                    column='channelIdentity_channelId',
                    op=FilterOperator.EQUAL,
                    value="mock_channel_id"
                ),
                FilterCondition(
                    column='id',
                    op=FilterOperator.EQUAL,
                    value="mock_id"
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_message]))


class TestMSTeamsChatsTable(MSTeamsResourceTestSetup, unittest.TestCase):
    def create_resource(self):
        return ChatsTable(self.handler)

    @patch('requests.get')
    def test_list_all(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for all chats.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_channel = self.generate_mock_data()
        mock_response.json.return_value = {
            "value": [
                mock_channel
            ]
        }
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_channel]))

    @patch('requests.get')
    def test_list_with_chat_id(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for a specific chat.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_chat = self.generate_mock_data()
        mock_response.json.return_value = mock_chat
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='id',
                    op=FilterOperator.EQUAL,
                    value="mock_id"
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_chat]))


class TestMSTeamsChatMessagesTable(MSTeamsResourceTestSetup, unittest.TestCase):
    def create_resource(self):
        return ChatMessagesTable(self.handler)

    def test_list_without_chat_id_raises_error(self):
        """"
        Test if `list` method raises a ValueError when chatId is not provided.
        """
        with self.assertRaises(ValueError, msg="The 'chatIdentity_chatId' column is required."):
            self.resource.list(conditions=[])

    @patch('requests.get')
    def test_list_with_chat_id(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for all messages in a specific chat.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_message = self.generate_mock_data()
        mock_response.json.return_value = {
            "value": [
                mock_message
            ]
        }
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='chatId',
                    op=FilterOperator.EQUAL,
                    value="mock_chat_id"
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_message]))

    @patch('requests.get')
    def test_list_with_chat_id_and_message_id(self, mock_get):
        """"
        Test if `list` method successfully returns a pandas DataFrame with data for a specific message in a specific chat.
        """
        mock_response = MagicMock(
            status_code=200
        )

        mock_message = self.generate_mock_data()
        mock_response.json.return_value = mock_message
        mock_get.return_value = mock_response

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='chatId',
                    op=FilterOperator.EQUAL,
                    value="mock_chat_id"
                ),
                FilterCondition(
                    column='id',
                    op=FilterOperator.EQUAL,
                    value="mock_id"
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        pd.testing.assert_frame_equal(response, pd.DataFrame([mock_message]))


if __name__ == '__main__':
    unittest.main()
