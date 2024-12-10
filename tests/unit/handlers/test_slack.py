from collections import OrderedDict
from copy import deepcopy
import datetime as dt
import unittest
from unittest.mock import MagicMock, patch

from mindsdb_sql_parser.ast import BinaryOperation, Constant, Delete, Identifier, Insert, Update
import pandas as pd
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse

from base_handler_test import BaseAPIChatHandlerTest, BaseAPIResourceTestSetup
from mindsdb.integrations.handlers.slack_handler.slack_handler import SlackHandler
from mindsdb.integrations.handlers.slack_handler.slack_tables import (
    SlackConversationsTable,
    SlackMessagesTable,
    SlackThreadsTable,
    SlackUsersTable
)
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

# Mock response for the first call to the `conversations.history` method.
MOCK_RESPONSE_CONV_HISTORY_1 = {
    "ok": True,
    "messages": [
        {
            "type": "message",
            "user": "U123ABC456",
            "text": "I find you punny and would like to smell your nose letter",
            "ts": "1512085950.000216"
        },
        {
            "type": "message",
            "user": "U222BBB222",
            "text": "What, you want to smell my shoes better?",
            "ts": "1512104434.000490"
        }
    ],
    "has_more": True,
    "pin_count": 0,
    "response_metadata": {
        "next_cursor": "bmV4dF90czoxNTEyMDg1ODYxMDAwNTQz"
    }
}

# Mock SlackResponse object for the first call to the `conversations.history` method.
MOCK_SLACK_RESPONSE_CONV_HISTORY_1 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/conversations.history",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_CONV_HISTORY_1)
)

# Mock response for the second call to the `conversations.history` method.
MOCK_RESPONSE_CONV_HISTORY_2 = {
    "ok": True,
    "messages": [
        {
            "type": "message",
            "user": "U222BBB222",
            "text": "Isn't this whether dreadful?",
            "ts": "1512104434.000490"
        },
    ],
    "has_more": False,
    "pin_count": 0,
    "response_metadata": {
        "next_cursor": ""
    }
}

# Mock SlackResponse object for the second call to the `conversations.history` method.
MOCK_SLACK_RESPONSE_CONV_HISTORY_2 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/conversations.history",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_CONV_HISTORY_2)
)

# Mock response for the first call to the `conversations.replies` method.
MOCK_RESPONSE_CONV_REPLIES_1 = {
    "messages": [
        {
            "type": "message",
            "user": "U061F7AUR",
            "text": "island",
            "thread_ts": "1482960137.003543",
            "reply_count": 3,
            "subscribed": True,
            "last_read": "1484678597.521003",
            "unread_count": 0,
            "ts": "1482960137.003543"
        },
        {
            "type": "message",
            "user": "U061F7AUR",
            "text": "one island",
            "thread_ts": "1482960137.003543",
            "parent_user_id": "U061F7AUR",
            "ts": "1483037603.017503"
        }
    ],
    "has_more": True,
    "ok": True,
    "response_metadata": {
        "next_cursor": "bmV4dF90czoxNDg0Njc4MjkwNTE3MDkx"
    }
}

# Mock SlackResponse object for the first call to the `conversations.replies` method.
MOCK_SLACK_RESPONSE_CONV_REPLIES_1 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/conversations.replies",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_CONV_REPLIES_1)
)

# Mock response for the second call to the `conversations.replies` method.
MOCK_RESPONSE_CONV_REPLIES_2 = {
    "messages": [
        {
            "type": "message",
            "user": "U061F7AUR",
            "text": "two island",
            "thread_ts": "1482960137.003543",
            "parent_user_id": "U061F7AUR",
            "ts": "1483051909.018632"
        },
        {
            "type": "message",
            "user": "U061F7AUR",
            "text": "three for the land",
            "thread_ts": "1482960137.003543",
            "parent_user_id": "U061F7AUR",
            "ts": "1483125339.020269"
        }
    ],
    "has_more": False,
    "ok": True,
    "response_metadata": {
        "next_cursor": ""
    }
}

# Mock SlackResponse object for the second call to the `conversations.replies` method.
MOCK_SLACK_RESPONSE_CONV_REPLIES_2 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/conversations.replies",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_CONV_REPLIES_2)
)

# Mock response for the first call to the `users.info` method.
MOCK_RESPONSE_USERS_LIST_1 = {
    "ok": True,
    "members": [
        {
            "id": "W012A3CDE",
            "team_id": "T012AB3C4",
            "name": "spengler",
            "deleted": False,
            "color": "9f69e7",
            "real_name": "spengler",
            "tz": "America/Los_Angeles",
            "tz_label": "Pacific Daylight Time",
            "tz_offset": -25200,
            "profile": {
                "avatar_hash": "ge3b51ca72de",
                "status_text": "Print is dead",
                "status_emoji": ":books:",
                "real_name": "Egon Spengler",
                "display_name": "spengler",
                "real_name_normalized": "Egon Spengler",
                "display_name_normalized": "spengler",
                "email": "spengler@ghostbusters.example.com",
                "image_24": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",
                "image_32": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",
                "image_48": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",
                "image_72": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",
                "image_192": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",
                "image_512": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",
                "team": "T012AB3C4"
            },
            "is_admin": True,
            "is_owner": False,
            "is_primary_owner": False,
            "is_restricted": False,
            "is_ultra_restricted": False,
            "is_bot": False,
            "updated": 1502138686,
            "is_app_user": False,
            "has_2fa": False
        }
    ],
    "cache_ts": 1498777272,
    "response_metadata": {
        "next_cursor": "dXNlcjpVMEc5V0ZYTlo="
    }
}

# Mock SlackResponse object for the first call to the `users.info` method.
MOCK_SLACK_RESPONSE_USERS_LIST_1 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/users.list",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_USERS_LIST_1)
)

# Mock response for the second call to the `users.info` method.
MOCK_RESPONSE_USERS_LIST_2 = {
    "ok": True,
    "members": [
        {
            "id": "W07QCRPA4",
            "team_id": "T0G9PQBBK",
            "name": "glinda",
            "deleted": False,
            "color": "9f69e7",
            "real_name": "Glinda Southgood",
            "tz": "America/Los_Angeles",
            "tz_label": "Pacific Daylight Time",
            "tz_offset": -25200,
            "profile": {
                "avatar_hash": "8fbdd10b41c6",
                "image_24": "https://a.slack-edge.com...png",
                "image_32": "https://a.slack-edge.com...png",
                "image_48": "https://a.slack-edge.com...png",
                "image_72": "https://a.slack-edge.com...png",
                "image_192": "https://a.slack-edge.com...png",
                "image_512": "https://a.slack-edge.com...png",
                "image_1024": "https://a.slack-edge.com...png",
                "image_original": "https://a.slack-edge.com...png",
                "first_name": "Glinda",
                "last_name": "Southgood",
                "title": "Glinda the Good",
                "phone": "",
                "skype": "",
                "real_name": "Glinda Southgood",
                "real_name_normalized": "Glinda Southgood",
                "display_name": "Glinda the Fairly Good",
                "display_name_normalized": "Glinda the Fairly Good",
                "email": "glenda@south.oz.coven"
            },
            "is_admin": True,
            "is_owner": False,
            "is_primary_owner": False,
            "is_restricted": False,
            "is_ultra_restricted": False,
            "is_bot": False,
            "updated": 1480527098,
            "has_2fa": False
        }
    ],
    "cache_ts": 1498777272,
    "response_metadata": {
        "next_cursor": ""
    }
}

# Mock SlackResponse object for the second call to the `users.info` method.
MOCK_SLACK_RESPONSE_USERS_LIST_2 = SlackResponse(
    client=MagicMock(),
    http_verb="GET",
    api_url="https://slack.com/api/users.list",
    req_args=MagicMock(),
    headers=MagicMock(),
    status_code=200,
    data=deepcopy(MOCK_RESPONSE_USERS_LIST_2)
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


class SlackAPIResourceTestSetup(BaseAPIResourceTestSetup):
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


class TestSlackConversationsTable(SlackAPIResourceTestSetup, unittest.TestCase):
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
                    value=MOCK_RESPONSE_CONV_INFO_1['channel']['id']
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


class TestSlackMessagesTable(SlackAPIResourceTestSetup, unittest.TestCase):
    def create_resource(self):
        return SlackMessagesTable(self.handler)

    def _get_expected_df_for_conv_history_1(self):
        """
        Returns the expected DataFrame for a single call to the `conversations_history` method.
        """
        mock_response_conv_history = deepcopy(MOCK_RESPONSE_CONV_HISTORY_1)

        expected_df_conv_history = pd.DataFrame(mock_response_conv_history['messages'], columns=self.resource.get_columns())
        expected_df_conv_history['created_at'] = pd.to_datetime(expected_df_conv_history['ts'].astype(float), unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

        expected_df_conv_history['channel_name'] = MOCK_RESPONSE_CONV_INFO_1['channel']['name']
        expected_df_conv_history['channel_id'] = MOCK_RESPONSE_CONV_INFO_1['channel']['id']

        return expected_df_conv_history

    def _get_expected_df_for_conv_history_2(self):
        """
        Returns the expected DataFrame for multiple(2) calls to the `conversations_history` method.
        """
        mock_response_conv_history_2 = deepcopy(MOCK_RESPONSE_CONV_HISTORY_2)

        expected_df_conv_history_1 = self._get_expected_df_for_conv_history_1()
        expected_df_conv_history_2 = pd.DataFrame(mock_response_conv_history_2['messages'], columns=self.resource.get_columns())
        expected_df_conv_history_2['created_at'] = pd.to_datetime(expected_df_conv_history_2['ts'].astype(float), unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

        expected_df_conv_history_2['channel_name'] = MOCK_RESPONSE_CONV_INFO_1['channel']['name']
        expected_df_conv_history_2['channel_id'] = MOCK_RESPONSE_CONV_INFO_1['channel']['id']

        return pd.concat([expected_df_conv_history_1, expected_df_conv_history_2], ignore_index=True)

    def test_list_with_channel_id_and_no_limit(self):
        """
        Tests the `list` method of the SlackMessagesTable class to ensure it correctly fetches the messages of a specific conversation without any limit.
        """
        self.mock_connect.return_value.conversations_history.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_HISTORY_1)
        self.mock_connect.return_value.conversations_info.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1)

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='channel_id',
                    op=FilterOperator.EQUAL,
                    value=MOCK_RESPONSE_CONV_INFO_1['channel']['id']
                )
            ]
        )

        self.assertEqual(self.mock_connect.return_value.conversations_history.call_count, 1)
        self.mock_connect.return_value.conversations_history.assert_any_call(
            channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'],
            limit=999
        )

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_history_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_channel_id_and_limit_less_than_999(self):
        """
        Tests the `list` method of the SlackMessagesTable class to ensure it correctly fetches the messages of a specific conversation with a limit less than 999.
        """
        self.mock_connect.return_value.conversations_history.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_HISTORY_1)

        self.mock_connect.return_value.conversations_info.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1)

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='channel_id',
                    op=FilterOperator.EQUAL,
                    value=MOCK_RESPONSE_CONV_INFO_1['channel']['id']
                )
            ],
            limit=998
        )

        self.assertEqual(self.mock_connect.return_value.conversations_history.call_count, 1)
        self.mock_connect.return_value.conversations_history.assert_any_call(
            channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'],
            limit=998
        )

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_history_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_channel_id_and_limit_more_than_999(self):
        """
        Tests the `list` method of the SlackMessagesTable class to ensure it correctly fetches the messages of a specific conversation with a limit more than 999.
        """
        self.mock_connect.return_value.conversations_history.side_effect = [
            deepcopy(MOCK_SLACK_RESPONSE_CONV_HISTORY_1),
            deepcopy(MOCK_SLACK_RESPONSE_CONV_HISTORY_2)
        ]

        self.mock_connect.return_value.conversations_info.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1)

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='channel_id',
                    op=FilterOperator.EQUAL,
                    value=MOCK_RESPONSE_CONV_INFO_1['channel']['id']
                )
            ],
            limit=1000
        )

        self.assertEqual(self.mock_connect.return_value.conversations_history.call_count, 2)
        self.mock_connect.return_value.conversations_history.assert_any_call(
            channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'],
        )
        self.mock_connect.return_value.conversations_history.assert_any_call(
            cursor=MOCK_RESPONSE_CONV_HISTORY_1['response_metadata']['next_cursor']
        )

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_history_2()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_without_channel_id(self):
        """
        Tests the `list` method raises a ValueError when the `channel_id` column is not included in the conditions.
        """
        with self.assertRaises(ValueError):
            self.resource.list(conditions=[])

    def test_list_with_channel_id_and_unsupported_operator(self):
        """
        Tests the `list` method raises a ValueError when an unsupported operator is used with the `channel_id` column.
        """
        with self.assertRaises(ValueError):
            self.resource.list(
                conditions=[
                    FilterCondition(
                        column='channel_id',
                        op=FilterOperator.IN,
                        value=[MOCK_RESPONSE_CONV_INFO_1['channel']['id']]
                    )
                ]
            )

    def test_insert_with_channel_id_and_text(self):
        """
        Tests the `insert` method of the SlackMessagesTable class to ensure it correctly sends a message to a specific conversation.
        """
        self.mock_connect.return_value.chat_postMessage.return_value = MagicMock()

        self.resource.insert(
            query=Insert(
                table='messages',
                columns=[
                    'channel_id',
                    'text'
                ],
                values=[
                    [
                        'C012AB3CD',
                        'Hello, World!'
                    ]
                ]
            )
        )

        self.assertEqual(self.mock_connect.return_value.chat_postMessage.call_count, 1)
        self.mock_connect.return_value.chat_postMessage.assert_any_call(
            channel='C012AB3CD',
            text='Hello, World!'
        )

    def test_insert_without_channel_id(self):
        """
        Tests the `insert` method raises a ValueError when the `channel_id` column is not included in the columns.
        """
        with self.assertRaises(ValueError):
            self.resource.insert(
                query=Insert(
                    table='messages',
                    columns=[
                        'text'
                    ],
                    values=[
                        [
                            'Hello, World!'
                        ]
                    ]
                )
            )

    def test_insert_without_text(self):
        """
        Tests the `insert` method raises a ValueError when the `text` column is not included in the columns.
        """
        with self.assertRaises(ValueError):
            self.resource.insert(
                query=Insert(
                    table='messages',
                    columns=[
                        'channel_id'
                    ],
                    values=[
                        [
                            'C012AB3CD'
                        ]
                    ]
                )
            )

    def test_update_with_channel_id_text_and_ts(self):
        """
        Tests the `update` method of the SlackMessagesTable class to ensure it correctly updates a message in a specific conversation.
        """
        self.mock_connect.return_value.chat_update.return_value = MagicMock()

        self.resource.update(
            query=Update(
                table='messages',
                update_columns={
                    'text': 'Hello, World!'
                },
                where=BinaryOperation(
                    op='and',
                    args=[
                        BinaryOperation(
                            op='=',
                            args=[
                                Identifier('channel_id'),
                                Constant('C012AB3CD')
                            ]
                        ),
                        BinaryOperation(
                            op='=',
                            args=[
                                Identifier('ts'),
                                Constant('1512085950.000216')
                            ]
                        )
                    ]
                )
            )
        )

        self.assertEqual(self.mock_connect.return_value.chat_update.call_count, 1)
        self.mock_connect.return_value.chat_update.assert_any_call(
            channel='C012AB3CD',
            text='Hello, World!',
            ts='1512085950.000216'
        )

    def test_update_without_channel_id(self):
        """
        Tests the `update` method raises a ValueError when the `channel_id` column is not included in the conditions.
        """
        with self.assertRaises(ValueError):
            self.resource.update(
                query=Update(
                    table='messages',
                    update_columns={
                        'text': 'Hello, World!'
                    },
                    where=BinaryOperation(
                        op='=',
                        args=[
                            Identifier('ts'),
                            Constant('1512085950.000216')
                        ]
                    )
                )
            )

    def test_update_without_ts(self):
        """
        Tests the `update` method raises a ValueError when the `ts` column is not included in the conditions.
        """
        with self.assertRaises(ValueError):
            self.resource.update(
                query=Update(
                    table='messages',
                    update_columns={
                        'text': 'Hello, World!'
                    },
                    where=BinaryOperation(
                        op='=',
                        args=[
                            Identifier('channel_id'),
                            Constant('C012AB3CD')
                        ]
                    )
                )
            )

    def test_update_without_text(self):
        """
        Tests the `update` method raises a ValueError when the `text` column is not included in the update_columns.
        """
        with self.assertRaises(ValueError):
            self.resource.update(
                query=Update(
                    table='messages',
                    update_columns={},
                    where=BinaryOperation(
                        op='and',
                        args=[
                            BinaryOperation(
                                op='=',
                                args=[
                                    Identifier('channel_id'),
                                    Constant('C012AB3CD')
                                ]
                            ),
                            BinaryOperation(
                                op='=',
                                args=[
                                    Identifier('ts'),
                                    Constant('1512085950.000216')
                                ]
                            )
                        ]
                    )
                )
            )

    def test_delete_with_channel_id_and_ts(self):
        """
        Tests the `delete` method of the SlackMessagesTable class to ensure it correctly deletes a message in a specific conversation.
        """
        self.mock_connect.return_value.chat_delete.return_value = MagicMock()

        self.resource.delete(
            query=Delete(
                table='messages',
                where=BinaryOperation(
                    op='and',
                    args=[
                        BinaryOperation(
                            op='=',
                            args=[
                                Identifier('channel_id'),
                                Constant('C012AB3CD')
                            ]
                        ),
                        BinaryOperation(
                            op='=',
                            args=[
                                Identifier('ts'),
                                Constant('1512085950.000216')
                            ]
                        )
                    ]
                )
            )
        )

        self.assertEqual(self.mock_connect.return_value.chat_delete.call_count, 1)
        self.mock_connect.return_value.chat_delete.assert_any_call(
            channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'],
            ts=float('1512085950.000216')
        )

    def test_delete_without_channel_id(self):
        """
        Tests the `delete` method raises a ValueError when the `channel_id` column is not included in the conditions.
        """
        with self.assertRaises(ValueError):
            self.resource.delete(
                query=Delete(
                    table='messages',
                    where=BinaryOperation(
                        op='=',
                        args=[
                            Identifier('ts'),
                            Constant('1512085950.000216')
                        ]
                    )
                )
            )

    def test_delete_without_ts(self):
        """
        Tests the `delete` method raises a ValueError when the `ts` column is not included in the conditions.
        """
        with self.assertRaises(ValueError):
            self.resource.delete(
                query=Delete(
                    table='messages',
                    where=BinaryOperation(
                        op='=',
                        args=[
                            Identifier('channel_id'),
                            Constant('C012AB3CD')
                        ]
                    )
                )
            )


class TestSlackThreadsTable(SlackAPIResourceTestSetup, unittest.TestCase):
    def create_resource(self):
        return SlackThreadsTable(self.handler)

    def _get_expected_df_for_conv_replies_1(self):
        """
        Returns the expected DataFrame for a single call to the `conversations_replies` method.
        """
        mock_response_conv_replies = deepcopy(MOCK_RESPONSE_CONV_REPLIES_1)

        expected_df_conv_replies = pd.DataFrame(mock_response_conv_replies['messages'], columns=self.resource.get_columns())

        expected_df_conv_replies['channel_name'] = MOCK_RESPONSE_CONV_INFO_1['channel']['name']
        expected_df_conv_replies['channel_id'] = MOCK_RESPONSE_CONV_INFO_1['channel']['id']

        return expected_df_conv_replies

    def _get_expected_df_for_conv_replies_2(self):
        """
        Returns the expected DataFrame for multiple(2) calls to the `conversations_replies` method.
        """
        mock_response_conv_replies_2 = deepcopy(MOCK_RESPONSE_CONV_REPLIES_2)

        expected_df_conv_replies_1 = self._get_expected_df_for_conv_replies_1()
        expected_df_conv_replies_2 = pd.DataFrame(mock_response_conv_replies_2['messages'], columns=self.resource.get_columns())

        expected_df_conv_replies_2['channel_name'] = MOCK_RESPONSE_CONV_INFO_1['channel']['name']
        expected_df_conv_replies_2['channel_id'] = MOCK_RESPONSE_CONV_INFO_1['channel']['id']

        return pd.concat([expected_df_conv_replies_1, expected_df_conv_replies_2], ignore_index=True)

    def test_list_with_channel_id_thread_ts_and_no_limit(self):
        """
        Tests the `list` method of the SlackThreadsTable class to ensure it correctly fetches the replies of a specific thread without any limit.
        """
        self.mock_connect.return_value.conversations_replies.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_REPLIES_1)
        self.mock_connect.return_value.conversations_info.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1)

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='channel_id',
                    op=FilterOperator.EQUAL,
                    value=MOCK_RESPONSE_CONV_INFO_1['channel']['id']
                ),
                FilterCondition(
                    column='thread_ts',
                    op=FilterOperator.EQUAL,
                    value='1482960137.003543'
                )
            ]
        )

        self.assertEqual(self.mock_connect.return_value.conversations_replies.call_count, 1)
        self.mock_connect.return_value.conversations_replies.assert_any_call(
            channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'],
            ts='1482960137.003543',
            limit=1000
        )

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_replies_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_channel_id_thread_ts_and_limit_less_than_1000(self):
        """
        Tests the `list` method of the SlackThreadsTable class to ensure it correctly fetches the replies of a specific thread with a limit less than 1000.
        """
        self.mock_connect.return_value.conversations_replies.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_REPLIES_1)
        self.mock_connect.return_value.conversations_info.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1)

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='channel_id',
                    op=FilterOperator.EQUAL,
                    value=MOCK_RESPONSE_CONV_INFO_1['channel']['id']
                ),
                FilterCondition(
                    column='thread_ts',
                    op=FilterOperator.EQUAL,
                    value='1482960137.003543'
                )
            ],
            limit=999
        )

        self.assertEqual(self.mock_connect.return_value.conversations_replies.call_count, 1)
        self.mock_connect.return_value.conversations_replies.assert_any_call(
            channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'],
            ts='1482960137.003543',
            limit=999
        )

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_replies_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_channel_id_thread_ts_and_limit_more_than_1000(self):
        """
        Tests the `list` method of the SlackThreadsTable class to ensure it correctly fetches the replies of a specific thread with a limit more than 1000.
        """
        self.mock_connect.return_value.conversations_replies.side_effect = [
            deepcopy(MOCK_SLACK_RESPONSE_CONV_REPLIES_1),
            deepcopy(MOCK_SLACK_RESPONSE_CONV_REPLIES_2)
        ]

        self.mock_connect.return_value.conversations_info.return_value = deepcopy(MOCK_SLACK_RESPONSE_CONV_INFO_1)

        response = self.resource.list(
            conditions=[
                FilterCondition(
                    column='channel_id',
                    op=FilterOperator.EQUAL,
                    value=MOCK_RESPONSE_CONV_INFO_1['channel']['id']
                ),
                FilterCondition(
                    column='thread_ts',
                    op=FilterOperator.EQUAL,
                    value='1482960137.003543'
                )
            ],
            limit=1001
        )

        self.assertEqual(self.mock_connect.return_value.conversations_replies.call_count, 2)
        self.mock_connect.return_value.conversations_replies.assert_any_call(
            channel=MOCK_RESPONSE_CONV_INFO_1['channel']['id'],
            ts='1482960137.003543'
        )
        self.mock_connect.return_value.conversations_replies.assert_any_call(
            cursor=MOCK_RESPONSE_CONV_REPLIES_1['response_metadata']['next_cursor']
        )

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_conv_replies_2()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_without_channel_id(self):
        """
        Tests the `list` method raises a ValueError when the `channel_id` column is not included in the conditions.
        """
        with self.assertRaises(ValueError):
            self.resource.list(conditions=[
                FilterCondition(
                    column='thread_ts',
                    op=FilterOperator.EQUAL,
                    value='1482960137.003543'
                )
            ])

    def test_list_with_channel_id_and_unsupported_operator(self):
        """
        Tests the `list` method raises a ValueError when an unsupported operator is used with the `channel_id` column.
        """
        with self.assertRaises(ValueError):
            self.resource.list(
                conditions=[
                    FilterCondition(
                        column='channel_id',
                        op=FilterOperator.IN,
                        value=[MOCK_RESPONSE_CONV_INFO_1['channel']['id']]
                    )
                ]
            )

    def test_list_without_thread_ts(self):
        """
        Tests the `list` method raises a ValueError when the `thread_ts` column is not included in the conditions.
        """
        with self.assertRaises(ValueError):
            self.resource.list(
                conditions=[
                    FilterCondition(
                        column='channel_id',
                        op=FilterOperator.EQUAL,
                        value=MOCK_RESPONSE_CONV_INFO_1['channel']['id']
                    )
                ]
            )

    def test_insert_with_channel_id_thread_ts_and_text(self):
        """
        Tests the `insert` method of the SlackThreadsTable class to ensure it correctly sends a reply to a specific thread.
        """
        self.mock_connect.return_value.chat_postMessage.return_value = MagicMock()

        self.resource.insert(
            query=Insert(
                table='threads',
                columns=[
                    'channel_id',
                    'thread_ts',
                    'text'
                ],
                values=[
                    [
                        'C012AB3CD',
                        '1482960137.003543',
                        'Hello, World!'
                    ]
                ]
            )
        )

        self.assertEqual(self.mock_connect.return_value.chat_postMessage.call_count, 1)
        self.mock_connect.return_value.chat_postMessage.assert_any_call(
            channel='C012AB3CD',
            thread_ts='1482960137.003543',
            text='Hello, World!'
        )

    def test_insert_without_channel_id(self):
        """
        Tests the `insert` method raises a ValueError when the `channel_id` column is not included in the columns.
        """
        with self.assertRaises(ValueError):
            self.resource.insert(
                query=Insert(
                    table='threads',
                    columns=[
                        'thread_ts',
                        'text'
                    ],
                    values=[
                        [
                            '1482960137.003543',
                            'Hello, World!'
                        ]
                    ]
                )
            )

    def test_insert_without_thread_ts(self):
        """
        Tests the `insert` method raises a ValueError when the `thread_ts` column is not included in the columns.
        """
        with self.assertRaises(ValueError):
            self.resource.insert(
                query=Insert(
                    table='threads',
                    columns=[
                        'channel_id',
                        'text'
                    ],
                    values=[
                        [
                            'C012AB3CD',
                            'Hello, World!'
                        ]
                    ]
                )
            )

    def test_insert_without_text(self):
        """
        Tests the `insert` method raises a ValueError when the `text` column is not included in the columns.
        """
        with self.assertRaises(ValueError):
            self.resource.insert(
                query=Insert(
                    table='threads',
                    columns=[
                        'channel_id',
                        'thread_ts'
                    ],
                    values=[
                        [
                            'C012AB3CD',
                            '1482960137.003543'
                        ]
                    ]
                )
            )


class TestSlackUsersTable(SlackAPIResourceTestSetup, unittest.TestCase):
    def create_resource(self):
        return SlackUsersTable(self.handler)

    def _get_expected_df_for_users_list_1(self):
        """
        Returns the expected DataFrame for a single call to the `users_list` method.
        """
        mock_response_users_list = deepcopy(MOCK_RESPONSE_USERS_LIST_1)

        expected_df_users_list = pd.DataFrame(mock_response_users_list['members'], columns=self.resource.get_columns())

        return expected_df_users_list

    def _get_expected_df_for_users_list_2(self):
        """
        Returns the expected DataFrame for multiple(2) calls to the `users_list` method.
        """
        mock_response_users_list_2 = deepcopy(MOCK_RESPONSE_USERS_LIST_2)

        expected_df_users_list_1 = self._get_expected_df_for_users_list_1()
        expected_df_users_list_2 = pd.DataFrame(mock_response_users_list_2['members'], columns=self.resource.get_columns())

        return pd.concat([expected_df_users_list_1, expected_df_users_list_2], ignore_index=True)

    def test_list_with_no_limit(self):
        """
        Tests the `list` method of the SlackUsersTable class to ensure it correctly fetches the details of all users without any limit.
        """
        self.mock_connect.return_value.users_list.return_value = deepcopy(MOCK_SLACK_RESPONSE_USERS_LIST_1)

        response = self.resource.list()

        self.assertEqual(self.mock_connect.return_value.users_list.call_count, 1)
        self.mock_connect.return_value.users_list.assert_any_call(limit=1000)

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_users_list_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_limit_less_than_1000(self):
        """
        Tests the `list` method of the SlackUsersTable class to ensure it correctly fetches the details of all users with a limit less than 1000.
        """
        self.mock_connect.return_value.users_list.return_value = deepcopy(MOCK_SLACK_RESPONSE_USERS_LIST_1)

        response = self.resource.list(limit=999)

        self.assertEqual(self.mock_connect.return_value.users_list.call_count, 1)
        self.mock_connect.return_value.users_list.assert_any_call(limit=999)

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_users_list_1()
        pd.testing.assert_frame_equal(response, expected_df)

    def test_list_with_limit_more_than_1000(self):
        """
        Tests the `list` method of the SlackUsersTable class to ensure it correctly fetches the details of all users with a limit more than 1000.
        """
        self.mock_connect.return_value.users_list.side_effect = [
            deepcopy(MOCK_SLACK_RESPONSE_USERS_LIST_1),
            deepcopy(MOCK_SLACK_RESPONSE_USERS_LIST_2)
        ]

        response = self.resource.list(limit=1001)

        self.assertEqual(self.mock_connect.return_value.users_list.call_count, 2)
        self.mock_connect.return_value.users_list.assert_any_call()
        self.mock_connect.return_value.users_list.assert_any_call(cursor=MOCK_RESPONSE_USERS_LIST_1['response_metadata']['next_cursor'])

        assert isinstance(response, pd.DataFrame)
        expected_df = self._get_expected_df_for_users_list_2()
        pd.testing.assert_frame_equal(response, expected_df)


if __name__ == '__main__':
    unittest.main()
