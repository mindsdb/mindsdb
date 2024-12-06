from collections import OrderedDict
import unittest
from unittest.mock import MagicMock, patch

from slack_sdk.errors import SlackApiError

from base_handler_test import BaseAPIChatHandlerTest
from mindsdb.integrations.handlers.slack_handler.slack_handler import SlackHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse


class TestSlackHandler(BaseAPIChatHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            token='xoxb-111-222-xyz',
            app_token='xapp-A111-222-xyz'
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return SlackApiError("Connection Failed", response=MagicMock())

    @property
    def registered_tables(self):
        return ['conversations', 'messages', 'threads', 'users']

    def create_handler(self):
        return SlackHandler('slack', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mindsdb.integrations.handlers.slack_handler.slack_handler.WebClient')
    
    @patch('mindsdb.integrations.handlers.slack_handler.slack_handler.SocketModeClient')
    def test_check_connection_success(self, mock_socket_mode_client):
        """
        Tests if the `check_connection` method handles a successful connection check and returns a StatusResponse object that accurately reflects the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertTrue(response.success)
        self.assertFalse(response.error_message)
    
    def test_get_my_user_name(self):
        """
        Tests the `get_my_user_name` method to ensure it correctly returns a username.
        """
        self.mock_connect.return_value.auth_test.return_value.data = {
            "ok": True,
            "url": "https://subarachnoid.slack.com/",
            "team": "Subarachnoid Workspace",
            "user": "bot",
            "team_id": "T0G9PQBBK",
            "user_id": "W23456789",
            "bot_id": "BZYBOTHED"
        }

        response = self.handler.get_my_user_name()
        assert response == "BZYBOTHED"


if __name__ == '__main__':
    unittest.main()
