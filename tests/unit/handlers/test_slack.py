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


# Mock response for the first call to the `conversations.info` method.
MOCK_RESPONSE_CONV_INFO_1 = {
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

# Mock SlackResponse object for the first call to the `conversations.info` method.
MOCK_SLACK_RESPONSE_CONV_INFO_1 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/conversations.info",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_CONV_INFO_1)
)

# Mock response for the second call to the `conversations.info` method.
MOCK_RESPONSE_CONV_INFO_2 = {
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

# Mock SlackResponse object for the second call to the `conversations.info` method.
MOCK_SLACK_RESPONSE_CONV_INFO_2 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/conversations.info",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_CONV_INFO_2)
)

# Mock response for the first call to the `conversations.list` method.
MOCK_RESPONSE_CONV_LIST_1 = {
    "ok": True,
    "channels": [
        MOCK_RESPONSE_CONV_INFO_1['channel']
    ],
    "response_metadata": {
        "next_cursor": "dGVhbTpDMDYxRkE1UEI="
    }
}

# Mock SlackResponse object for the first call to the `conversations.list` method.
MOCK_SLACK_RESPONSE_CONV_LIST_1 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/conversations.list",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_CONV_LIST_1)
)

# Mock response for the second call to the `conversations.list` method.
MOCK_RESPONSE_CONV_LIST_2 = {
    "ok": True,
    "channels": [
        MOCK_RESPONSE_CONV_INFO_2['channel']
    ],
    "response_metadata": {
        "next_cursor": ""
    }
}

# Mock SlackResponse object for the second call to the `conversations.list` method.
MOCK_SLACK_RESPONSE_CONV_LIST_2 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/conversations.list",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_CONV_LIST_2)
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
        self.mock_connect.return_value.conversations_info.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1)

        query = "conversations_info(channel='C1234567890')"
        response = self.handler.native_query(query)

        self.mock_connect.return_value.conversations_info.assert_called_once_with(channel='C1234567890')
        assert isinstance(response, Response)
        expected_df = pd.DataFrame([MOCK_RESPONSE_CONV_INFO_1['channel']])
        pd.testing.assert_frame_equal(response.data_frame, expected_df)

    def test_native_query_with_pagination(self):
        """
        Tests the `native_query` method to ensure it executes a Slack API method with pagination and returns a Response object.
        """
        self.mock_connect.return_value.conversations_list.side_effect = [
            deepcopy(MOCK_SLACK_RESPONSE_CONV_LIST_1),
            deepcopy(MOCK_SLACK_RESPONSE_CONV_LIST_2)
        ]

        query = "conversations_list()"
        response = self.handler.native_query(query)

        self.assertEqual(self.mock_connect.return_value.conversations_list.call_count, 2)
        self.mock_connect.return_value.conversations_list.assert_any_call()
        self.mock_connect.return_value.conversations_list.assert_any_call(cursor="dGVhbTpDMDYxRkE1UEI=")

        assert isinstance(response, Response)
        expected_df = pd.DataFrame(MOCK_RESPONSE_CONV_LIST_1['channels'] + MOCK_RESPONSE_CONV_LIST_2['channels'])
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

    def _get_expected_df_for_conv_info_1(self):
        """
        Returns the expected DataFrame for a single call to the `conversations_info` method.
        """
        mock_response_conv_info = deepcopy(MOCK_RESPONSE_CONV_INFO_1)
        mock_response_conv_info['channel']['created_at'] = dt.datetime.fromtimestamp(mock_response_conv_info['channel']['created'])
        mock_response_conv_info['channel']['updated_at'] = dt.datetime.fromtimestamp(mock_response_conv_info['channel']['updated'] / 1000)

        return pd.DataFrame([mock_response_conv_info['channel']], columns=self.resource.get_columns())

    def _get_expected_df_for_conv_info_2(self):
        """
        Returns the expected DataFrame for multiple(2) calls to the `conversations_info` method.
        """
        mock_response_conv_info = deepcopy(MOCK_RESPONSE_CONV_INFO_2)
        mock_response_conv_info['channel']['created_at'] = dt.datetime.fromtimestamp(mock_response_conv_info['channel']['created'])
        mock_response_conv_info['channel']['updated_at'] = dt.datetime.fromtimestamp(mock_response_conv_info['channel']['updated'] / 1000)

        expected_df_conv_info_1 = self._get_expected_df_for_conv_info_1()
        expected_df_conv_info_2 = pd.DataFrame([mock_response_conv_info['channel']], columns=self.resource.get_columns())

        return pd.concat([expected_df_conv_info_1, expected_df_conv_info_2], ignore_index=True)

    def _get_expected_df_for_conv_list_1(self):
        """
        Returns the expected DataFrame for a single call to the `conversations_list` method.
        """
        mock_response_conv_list = deepcopy(MOCK_RESPONSE_CONV_LIST_1)
        mock_response_conv_list['channels'][0]['created_at'] = dt.datetime.fromtimestamp(mock_response_conv_list['channels'][0]['created'])
        mock_response_conv_list['channels'][0]['updated_at'] = dt.datetime.fromtimestamp(mock_response_conv_list['channels'][0]['updated'] / 1000)

        return pd.DataFrame([mock_response_conv_list['channels'][0]], columns=self.resource.get_columns())

    def _get_expected_df_for_conv_list_2(self):
        """
        Returns the expected DataFrame for multiple(2) calls to the `conversations_list` method.
        """
        mock_response_conv_list = deepcopy(MOCK_RESPONSE_CONV_LIST_2)
        mock_response_conv_list['channels'][0]['created_at'] = dt.datetime.fromtimestamp(mock_response_conv_list['channels'][0]['created'])
        mock_response_conv_list['channels'][0]['updated_at'] = dt.datetime.fromtimestamp(mock_response_conv_list['channels'][0]['updated'] / 1000)

        expected_df_conv_list_1 = self._get_expected_df_for_conv_list_1()
        expected_df_conv_list_2 = pd.DataFrame([mock_response_conv_list['channels'][0]], columns=self.resource.get_columns())

        return pd.concat([expected_df_conv_list_1, expected_df_conv_list_2], ignore_index=True)

    def test_list_with_channel_id(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches the details of a specific conversation.
        """
        self.mock_connect.return_value.conversations_info.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1)

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='id',
                    op=FilterOperator.EQUAL,
                    value='C012AB3CD'
                )
            ]
        )

        self.assertEqual(self.mock_connect.return_value.conversations_info.call_count, 1)
        self.mock_connect.return_value.conversations_info.assert_any_call(channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'])

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_info_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_multiple_channel_ids(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches the details of multiple conversations.
        """
        self.mock_connect.return_value.conversations_info.side_effect = [
            deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1),
            deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_2)
        ]

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='id',
                    op=FilterOperator.IN,
                    value=[MOCK_RESPONSE_CONV_INFO_1['channel']['id'], MOCK_RESPONSE_CONV_INFO_2['channel']['id']]
                )
            ]
        )

        self.assertEqual(self.mock_connect.return_value.conversations_info.call_count, 2)
        self.mock_connect.return_value.conversations_info.assert_any_call(channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'])
        self.mock_connect.return_value.conversations_info.assert_any_call(channel=MOCK_RESPONSE_CONV_INFO_2['channel']['id'])

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_info_2()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_no_conditions_and_no_limit(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches a list of conversations without any conditions or limits.
        """
        self.mock_connect.return_value.conversations_list.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_LIST_1)

        response = self.resource.list(
            conditions=[]
        )

        self.assertEqual(self.mock_connect.return_value.conversations_list.call_count, 1)
        self.mock_connect.return_value.conversations_list.assert_any_call(limit=1000)

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_list_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_no_conditions_and_limit_less_than_1000(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches a list of conversations without any conditions and with a limit less than 1000.
        """
        self.mock_connect.return_value.conversations_list.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_LIST_1)

        response = self.resource.list(
            conditions=[],
            limit=999
        )

        self.assertEqual(self.mock_connect.return_value.conversations_list.call_count, 1)
        self.mock_connect.return_value.conversations_list.assert_any_call(limit=999)

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_list_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_no_conditions_and_limit_more_than_1000(self):
        """
        Tests the `list` method of the SlackConversationsTable class to ensure it correctly fetches a list of conversations without any conditions and with a limit more than 1000.
        """
        self.mock_connect.return_value.conversations_list.side_effect = [
            deepcopy(MOCK_SLACK_RESPONSE_CONV_LIST_1),
            deepcopy(MOCK_SLACK_RESPONSE_CONV_LIST_2)
        ]

        response = self.resource.list(
            conditions=[],
            limit=1001
        )

        self.assertEqual(self.mock_connect.return_value.conversations_list.call_count, 2)
        self.mock_connect.return_value.conversations_list.assert_any_call()
        self.mock_connect.return_value.conversations_list.assert_any_call(cursor="dGVhbTpDMDYxRkE1UEI=")

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_list_2()
        pd.testing.assert_frame_equal(response, expected_df)


if __name__ == '__main__':
    unittest.main()
