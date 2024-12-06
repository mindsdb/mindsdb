from collections import OrderedDict
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse

from base_handler_test import BaseAPIChatHandlerTest
from mindsdb.integrations.handlers.slack_handler.slack_handler import SlackHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response
)


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

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a Slack API method and returns a Response object.
        """
        mock_response = {
            "ok": True,
            "channel": {
                "id": "C012AB3CD",
                "name": "general",
                "is_channel": True,
                "is_group": False,
                "is_im": False,
                "is_mpim": False,
                "is_private": False,
            }
        }
        slack_response = SlackResponse(
            client=MagicMock(),
            http_verb="GET",
            api_url="https://slack.com/api/conversations.info",
            req_args=MagicMock(),
            headers=MagicMock(),
            status_code=200,
            data=mock_response
        )
        self.mock_connect.return_value.conversations_info.return_value = slack_response

        query = "conversations_info(channel='C1234567890')"
        response = self.handler.native_query(query)

        self.mock_connect.return_value.conversations_info.assert_called_once_with(channel='C1234567890')
        assert isinstance(response, Response)
        pd.testing.assert_frame_equal(
            response.data_frame,
            pd.DataFrame(
                [mock_response['channel']],
            )
        )

    def test_native_query_with_pagination(self):
        """
        Tests the `native_query` method to ensure it executes a Slack API method with pagination and returns a Response object.
        """
        mock_response_page_1 = {
            "ok": True,
            "channels": [
                {
                    "id": "C012AB3CD",
                    "name": "general",
                    "is_channel": True,
                    "is_group": False,
                    "is_im": False,
                    "is_mpim": False,
                    "is_private": False,
                }
            ],
            "response_metadata": {
                "next_cursor": "dGVhbTpDMDYxRkE1UEI="
            }
        }
        slack_response_page_1 = SlackResponse(
            client=MagicMock(),
            http_verb="GET",
            api_url="https://slack.com/api/conversations.list",
            req_args=MagicMock(),
            headers=MagicMock(),
            status_code=200,
            data=mock_response_page_1
        )

        mock_response_page_2 = {
            "ok": True,
            "channels": [
                {
                    "id": "C012AB3CE",
                    "name": "random",
                    "is_channel": True,
                    "is_group": False,
                    "is_im": False,
                    "is_mpim": False,
                    "is_private": False,
                }
            ],
            "response_metadata": {
                "next_cursor": ""
            }
        }
        slack_response_page_2 = SlackResponse(
            client=MagicMock(),
            http_verb="GET",
            api_url="https://slack.com/api/conversations.list",
            req_args=MagicMock(),
            headers=MagicMock(),
            status_code=200,
            data=mock_response_page_2
        )

        self.mock_connect.return_value.conversations_list.side_effect = [
            slack_response_page_1,
            slack_response_page_2
        ]

        query = "conversations_list()"
        response = self.handler.native_query(query)

        self.assertEqual(self.mock_connect.return_value.conversations_list.call_count, 2)
        self.mock_connect.return_value.conversations_list.assert_any_call()
        self.mock_connect.return_value.conversations_list.assert_any_call(cursor="dGVhbTpDMDYxRkE1UEI=")

        assert isinstance(response, Response)
        expected_df = pd.DataFrame(mock_response_page_1['channels'] + mock_response_page_2['channels'])
        pd.testing.assert_frame_equal(response.data_frame, expected_df)

    @patch('mindsdb.integrations.handlers.slack_handler.slack_handler.SocketModeClient')
    def test_subscribe(self, mock_socket_mode_client):
        """
        Tests the `subscribe` method to ensure it processes Slack events correctly.
        """
        pass


if __name__ == '__main__':
    unittest.main()
