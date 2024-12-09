from collections import OrderedDict
from copy import deepcopy
import datetime as dt
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse

from base_handler_test import BaseAPIChatHandlerTest, BaseAPIResourceTestSetup
from mindsdb.integrations.handlers.slack_handler.slack_handler import SlackHandler
from mindsdb.integrations.handlers.slack_handler.slack_tables import SlackConversationsTable
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response
)
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


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

    def test_subscribe(self):
        """
        Tests the `subscribe` method to ensure it processes Slack events correctly.
        """
        pass


class TestSlackConversationsTable(BaseAPIResourceTestSetup, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(
            token='xoxb-111-222-xyz',
            app_token='xapp-A111-222-xyz'
        )

    def create_handler(self):
        return SlackHandler('slack', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mindsdb.integrations.handlers.slack_handler.slack_handler.WebClient')

    def create_resource(self):
        return SlackConversationsTable(self.handler)

    def test_list_with_channel_id(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches the details of a specific conversation.
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
                "created": 1449252889,
                "updated": 1678229664302,
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

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='id',
                    op=FilterOperator.EQUAL,
                    value='C012AB3CD'
                )
            ]
        )

        assert isinstance(response, pd.DataFrame)
        mock_response['channel']['created_at'] = dt.datetime.fromtimestamp(mock_response['channel']['created'])
        mock_response['channel']['updated_at'] = dt.datetime.fromtimestamp(mock_response['channel']['updated'] / 1000)
        expected_df = pd.DataFrame([mock_response['channel']], columns=self.resource.get_columns())
        pd.testing.assert_frame_equal(
            response,
            expected_df
        )

    def test_list_with_multiple_channel_ids(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches the details of multiple conversations.
        """
        mock_response_page_1 = {
            "ok": True,
            "channel": {
                "id": "C012AB3CD",
                "name": "general",
                "is_channel": True,
                "is_group": False,
                "is_im": False,
                "is_mpim": False,
                "is_private": False,
                "created": 1449252889,
                "updated": 1678229664302,
            }
        }
        slack_response_page_1 = SlackResponse(
            client=MagicMock(),
            http_verb="GET",
            api_url="https://slack.com/api/conversations.info",
            req_args=MagicMock(),
            headers=MagicMock(),
            status_code=200,
            data=mock_response_page_1
        )

        mock_response_page_2 = {
            "ok": True,
            "channel": {
                "id": "C012AB3CE",
                "name": "random",
                "is_channel": True,
                "is_group": False,
                "is_im": False,
                "is_mpim": False,
                "is_private": False,
                "created": 1449252889,
                "updated": 1678229664302,
            }
        }
        slack_response_page_2 = SlackResponse(
            client=MagicMock(),
            http_verb="GET",
            api_url="https://slack.com/api/conversations.info",
            req_args=MagicMock(),
            headers=MagicMock(),
            status_code=200,
            data=mock_response_page_2
        )

        self.mock_connect.return_value.conversations_info.side_effect = [
            slack_response_page_1,
            slack_response_page_2
        ]

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='id',
                    op=FilterOperator.IN,
                    value=['C012AB3CD', 'C012AB3CE']
                )
            ]
        )

        self.assertEqual(self.mock_connect.return_value.conversations_info.call_count, 2)
        self.mock_connect.return_value.conversations_info.assert_any_call(channel='C012AB3CD')
        self.mock_connect.return_value.conversations_info.assert_any_call(channel='C012AB3CE')

        assert isinstance(response, pd.DataFrame)
        mock_response_page_1['channel']['created_at'] = dt.datetime.fromtimestamp(mock_response_page_1['channel']['created'])
        mock_response_page_1['channel']['updated_at'] = dt.datetime.fromtimestamp(mock_response_page_1['channel']['updated'] / 1000)
        mock_response_page_2['channel']['created_at'] = dt.datetime.fromtimestamp(mock_response_page_2['channel']['created'])
        mock_response_page_2['channel']['updated_at'] = dt.datetime.fromtimestamp(mock_response_page_2['channel']['updated'] / 1000)
        expected_df = pd.DataFrame([mock_response_page_1['channel'], mock_response_page_2['channel']], columns=self.resource.get_columns())
        pd.testing.assert_frame_equal(
            response,
            expected_df
        )

    def test_list_with_no_conditions_and_no_limit(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches a list of conversations without any conditions or limits.
        """
        mock_response = {
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
                    "created": 1449252889,
                    "updated": 1678229664302,
                }
            ],
            "response_metadata": {
                "next_cursor": "dGVhbTpDMDYxRkE1UEI="
            }
        }
        slack_response = SlackResponse(
            client=MagicMock(),
            http_verb="GET",
            api_url="https://slack.com/api/conversations.list",
            req_args=MagicMock(),
            headers=MagicMock(),
            status_code=200,
            data=mock_response
        )

        self.mock_connect.return_value.conversations_list.return_value = slack_response

        response = self.resource.list(
            conditions=[]
        )

        self.assertEqual(self.mock_connect.return_value.conversations_list.call_count, 1)
        self.mock_connect.return_value.conversations_list.assert_any_call(limit=1000)
        assert isinstance(response, pd.DataFrame)
        mock_response['channels'][0]['created_at'] = dt.datetime.fromtimestamp(mock_response['channels'][0]['created'])
        mock_response['channels'][0]['updated_at'] = dt.datetime.fromtimestamp(mock_response['channels'][0]['updated'] / 1000)
        expected_df = pd.DataFrame(mock_response['channels'], columns=self.resource.get_columns())
        pd.testing.assert_frame_equal(
            response,
            expected_df
        )

    def test_list_with_no_conditions_and_limit_less_than_1000(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches a list of conversations without any conditions and with a limit less than 1000.
        """
        mock_response = {
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
                    "created": 1449252889,
                    "updated": 1678229664302,
                }
            ]
        }
        slack_response = SlackResponse(
            client=MagicMock(),
            http_verb="GET",
            api_url="https://slack.com/api/conversations.list",
            req_args=MagicMock(),
            headers=MagicMock(),
            status_code=200,
            data=mock_response
        )

        self.mock_connect.return_value.conversations_list.return_value = slack_response

        response = self.resource.list(
            conditions=[],
            limit=999
        )

        self.assertEqual(self.mock_connect.return_value.conversations_list.call_count, 1)
        self.mock_connect.return_value.conversations_list.assert_any_call(limit=999)
        assert isinstance(response, pd.DataFrame)
        mock_response['channels'][0]['created_at'] = dt.datetime.fromtimestamp(mock_response['channels'][0]['created'])
        mock_response['channels'][0]['updated_at'] = dt.datetime.fromtimestamp(mock_response['channels'][0]['updated'] / 1000)
        expected_df = pd.DataFrame(mock_response['channels'], columns=self.resource.get_columns())
        pd.testing.assert_frame_equal(
            response,
            expected_df
        )

    def test_list_with_no_conditions_and_limit_more_than_1000(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches a list of conversations without any conditions and with a limit more than 1000.
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
                    "created": 1449252889,
                    "updated": 1678229664302,
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
            data=deepcopy(mock_response_page_1)
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
                    "created": 1449252889,
                    "updated": 1678229664302,
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
            data=deepcopy(mock_response_page_2)
        )

        self.mock_connect.return_value.conversations_list.side_effect = [
            slack_response_page_1,
            slack_response_page_2
        ]

        response = self.resource.list(
            conditions=[],
            limit=1001
        )

        self.assertEqual(self.mock_connect.return_value.conversations_list.call_count, 2)
        self.mock_connect.return_value.conversations_list.assert_any_call()
        self.mock_connect.return_value.conversations_list.assert_any_call(cursor="dGVhbTpDMDYxRkE1UEI=")

        assert isinstance(response, pd.DataFrame)
        mock_response_page_1['channels'][0]['created_at'] = dt.datetime.fromtimestamp(mock_response_page_1['channels'][0]['created'])
        mock_response_page_1['channels'][0]['updated_at'] = dt.datetime.fromtimestamp(mock_response_page_1['channels'][0]['updated'] / 1000)
        mock_response_page_2['channels'][0]['created_at'] = dt.datetime.fromtimestamp(mock_response_page_2['channels'][0]['created'])
        mock_response_page_2['channels'][0]['updated_at'] = dt.datetime.fromtimestamp(mock_response_page_2['channels'][0]['updated'] / 1000)
        expected_df = pd.DataFrame(mock_response_page_1['channels'] + mock_response_page_2['channels'], columns=self.resource.get_columns())
        pd.testing.assert_frame_equal(
            response,
            expected_df
        )


if __name__ == '__main__':
    unittest.main()
