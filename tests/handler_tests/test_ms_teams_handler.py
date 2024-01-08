import unittest
from unittest.mock import Mock, patch

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import Constant, BinaryOperation

from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_handler import MSTeamsHandler
from mindsdb.integrations.handlers.ms_teams_handler.settings import ms_teams_handler_config
from mindsdb.integrations.handlers.ms_teams_handler.ms_graph_api_teams_client import MSGraphAPITeamsClient
from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_tables import ChatsTable, ChatMessagesTable, ChannelsTable, ChannelMessagesTable


class TestMSGraphAPITeamsClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up the tests.
        """

        # mock the api client with a dummy access_token parameter (calls to the API that use this parameter will be mocked)
        cls.api_client = MSGraphAPITeamsClient("test_access_token")

    @patch('requests.get')
    def test_get_chat_returns_chat_data(self, mock_get):
        """
        Test that get_chat returns chat data.
        """

        # configure the mock to return a response with 'status_code' 200
        mock_get.return_value = Mock(
            status_code=200,
            headers={'Content-Type': 'application/json'},
            json=Mock(return_value=ms_teams_handler_config.TEST_CHAT_DATA)
        )

        chat_data = self.api_client.get_chat("test_id")

        # assert the requests.get call was made with the expected arguments
        mock_get.assert_called_once_with(
            'https://graph.microsoft.com/v1.0/chats/test_id/',
            headers={'Authorization': 'Bearer test_access_token'},
            params={'$expand': 'lastMessagePreview'}
        )

        self.assertEqual(chat_data["id"], "test_id")
        self.assertEqual(chat_data["chatType"], "oneOnOne")

    @patch('requests.get')
    def test_get_chats_returns_chats_data(self, mock_get):
        """
        Test that get_chats returns chats data.
        """

        # configure the mock to return a response with 'status_code' 200
        mock_get.return_value = Mock(
            status_code=200,
            headers={'Content-Type': 'application/json'},
            json=Mock(return_value={'value': ms_teams_handler_config.TEST_CHATS_DATA})
        )

        chats_data = self.api_client.get_chats()

        # assert the requests.get call was made with the expected arguments
        mock_get.assert_called_once_with(
            'https://graph.microsoft.com/v1.0/chats/',
            headers={'Authorization': 'Bearer test_access_token'},
            params={'$expand': 'lastMessagePreview', '$top': 20}
        )

        self.assertEqual(chats_data[0]["id"], "test_id")
        self.assertEqual(chats_data[0]["chatType"], "oneOnOne")

    @patch('requests.get')
    def test_get_chat_message_returns_chat_message_data(self, mock_get):
        """
        Test that get_chat_message returns chat message data.
        """

        # configure the mock to return a response with 'status_code' 200
        mock_get.return_value = Mock(
            status_code=200,
            headers={'Content-Type': 'application/json'},
            json=Mock(return_value=ms_teams_handler_config.TEST_CHAT_MESSAGE_DATA)
        )

        chat_message_data = self.api_client.get_chat_message("test_chat_id", "test_id")

        # assert the requests.get call was made with the expected arguments
        mock_get.assert_called_once_with(
            'https://graph.microsoft.com/v1.0/chats/test_chat_id/messages/test_id/',
            headers={'Authorization': 'Bearer test_access_token'},
            params=None
        )

        self.assertEqual(chat_message_data["id"], "test_id")
        self.assertEqual(chat_message_data["messageType"], "message")
        self.assertEqual(chat_message_data["chatId"], "test_chat_id")

    @patch('requests.get')
    def test_get_chat_messages_returns_chat_messages_data(self, mock_get):
        """
        Test that get_chat_messages returns chat messages data.
        """

        # configure the mock to return a response with 'status_code' 200
        mock_get.return_value = Mock(
            status_code=200,
            headers={'Content-Type': 'application/json'},
            json=Mock(return_value={'value': ms_teams_handler_config.TEST_CHAT_MESSAGES_DATA})
        )

        chat_messages_data = self.api_client.get_chat_messages("test_chat_id")

        # assert the requests.get call was made with the expected arguments
        mock_get.assert_called_once_with(
            'https://graph.microsoft.com/v1.0/chats/test_chat_id/messages/',
            headers={'Authorization': 'Bearer test_access_token'},
            params={'$top': 20}
        )

        self.assertEqual(chat_messages_data[0]["id"], "test_id")
        self.assertEqual(chat_messages_data[0]["messageType"], "message")
        self.assertEqual(chat_messages_data[0]["chatId"], "test_chat_id")

    @patch('requests.get')
    def test_get_all_chat_messages_returns_all_chat_messages_data(self, mock_get):
        """
        Test that get_all_chat_messages returns all chat messages data.
        """

        # configure the mock to return a response with 'status_code' 200
        mock_get.side_effect = [
            Mock(
                status_code=200,
                headers={'Content-Type': 'application/json'},
                json=Mock(return_value={'value': ms_teams_handler_config.TEST_CHATS_DATA})
            ),
            Mock(
                status_code=200,
                headers={'Content-Type': 'application/json'},
                json=Mock(return_value={'value': ms_teams_handler_config.TEST_CHAT_MESSAGES_DATA})
            )
        ]

        chat_messages_data = self.api_client.get_all_chat_messages()

        # assert the requests.get calls were made with the expected arguments
        mock_get.assert_any_call(
            'https://graph.microsoft.com/v1.0/chats/',
            headers={'Authorization': 'Bearer test_access_token'},
            params={'$expand': 'lastMessagePreview', '$top': 20}
        )

        mock_get.assert_any_call(
            'https://graph.microsoft.com/v1.0/chats/test_id/messages/',
            headers={'Authorization': 'Bearer test_access_token'},
            params={'$top': 20}
        )

        self.assertEqual(chat_messages_data[0]["id"], "test_id")
        self.assertEqual(chat_messages_data[0]["messageType"], "message")
        self.assertEqual(chat_messages_data[0]["chatId"], "test_chat_id")

    @patch('requests.post')
    def test_send_chat_message_sends_correct_request(self, mock_post):
        """
        Test that send_chat_message sends a chat message.
        """

        # configure the mock to return a response with 'status_code' 201
        mock_post.return_value = Mock(
            status_code=201,
            headers={'Content-Type': 'application/json'},
        )

        self.api_client.send_chat_message("test_chat_id", "test_message", "test_subject")

        # assert the requests.post call was made with the expected arguments
        mock_post.assert_called_once_with(
            'https://graph.microsoft.com/v1.0/chats/test_chat_id/messages/',
            headers={'Authorization': 'Bearer test_access_token'},
            json={'subject': 'test_subject', 'body': {'content': 'test_message'}}
        )

    @patch('requests.get')
    def test_get_channel_returns_channel_data(self, mock_get):
        """
        Test that get_channel returns channel data.
        """

        # configure the mock to return a response with 'status_code' 200
        mock_get.return_value = Mock(
            status_code=200,
            headers={'Content-Type': 'application/json'},
            json=Mock(return_value=ms_teams_handler_config.TEST_CHANNEL_DATA)
        )

        channel_data = self.api_client.get_channel("test_team_id", "test_id")

        # assert the requests.get call was made with the expected arguments
        mock_get.assert_called_once_with(
            'https://graph.microsoft.com/v1.0/teams/test_team_id/channels/test_id/',
            headers={'Authorization': 'Bearer test_access_token'},
            params=None
        )

        self.assertEqual(channel_data["id"], "test_id")
        self.assertEqual(channel_data["displayName"], "test_display_name")
        self.assertEqual(channel_data["teamId"], "test_team_id")

    @patch('requests.get')
    def test_get_channels_returns_channels_data(self, mock_get):
        """
        Test that get_channels returns channels data.
        """

        # check if the group_ids parameter in the API client is set
        is_group_ids_set = True if self.api_client._group_ids is not None else False

        # configure the mock to return a response with 'status_code' 200
        # if the group_ids parameter is not set, the mock will return the group data first, then the channels data
        if not is_group_ids_set:
            mock_get.side_effect = [
                Mock(
                    status_code=200,
                    headers={'Content-Type': 'application/json'},
                    json=Mock(return_value=ms_teams_handler_config.TEST_GROUP_DATA)
                ),
                Mock(
                    status_code=200,
                    headers={'Content-Type': 'application/json'},
                    json=Mock(return_value={'value': ms_teams_handler_config.TEST_CHANNELS_DATA})
                )
            ]

        # if the group_ids parameter is set, the mock will only return the channels data
        else:
            mock_get.return_value = Mock(
                status_code=200,
                headers={'Content-Type': 'application/json'},
                json=Mock(return_value={'value': ms_teams_handler_config.TEST_CHANNELS_DATA})
            )

        channels_data = self.api_client.get_channels()

        # assert the requests.get calls were made with the expected arguments
        # if the group_ids parameter is not set, the mock will check the calls to both the groups and channels endpoints
        if not is_group_ids_set:
            mock_get.assert_any_call(
                'https://graph.microsoft.com/v1.0/groups/',
                headers={'Authorization': 'Bearer test_access_token'},
                params={'$select': 'id,resourceProvisioningOptions'}
            )

            mock_get.assert_any_call(
                'https://graph.microsoft.com/v1.0/teams/test_team_id/channels/',
                headers={'Authorization': 'Bearer test_access_token'},
                params=None
            )

        # if the group_ids parameter is set, the mock will only check the call to the channels endpoint
        else:
            mock_get.assert_called_once_with(
                'https://graph.microsoft.com/v1.0/teams/test_team_id/channels/',
                headers={'Authorization': 'Bearer test_access_token'},
                params=None
            )

        self.assertEqual(channels_data[0]["id"], "test_id")
        self.assertEqual(channels_data[0]["displayName"], "test_display_name")
        self.assertEqual(channels_data[0]["teamId"], "test_team_id")

    @patch('requests.get')
    def test_get_channel_message_returns_channel_message_data(self, mock_get):
        """
        Test that get_channel_message returns channel message data.
        """

        # configure the mock to return a response with 'status_code' 200
        mock_get.return_value = Mock(
            status_code=200,
            headers={'Content-Type': 'application/json'},
            json=Mock(return_value=ms_teams_handler_config.TEST_CHANNEL_MESSAGE_DATA)
        )

        channel_message_data = self.api_client.get_channel_message("test_team_id", "test_channel_id", "test_id")

        # assert the requests.get call was made with the expected arguments
        mock_get.assert_called_once_with(
            'https://graph.microsoft.com/v1.0/teams/test_team_id/channels/test_channel_id/messages/test_id/',
            headers={'Authorization': 'Bearer test_access_token'},
            params=None
        )

        self.assertEqual(channel_message_data["id"], "test_id")
        self.assertEqual(channel_message_data["messageType"], "message")
        self.assertEqual(channel_message_data["channelIdentity"]["channelId"], "test_channel_id")
        self.assertEqual(channel_message_data["channelIdentity"]["teamId"], "test_team_id")

    @patch('requests.get')
    def test_get_channel_messages_returns_channel_messages_data(self, mock_get):
        """
        Test that get_channel_messages returns channel messages data.
        """

        # check if the group_ids parameter in the API client is set
        is_group_ids_set = True if self.api_client._group_ids is not None else False

        # configure the mocks to return a response with 'status_code' 200
        # if the group_ids parameter is not set, the mocks will return the group data first, then the channel ID data, then the channel messages data
        if not is_group_ids_set:
            mock_get.side_effect = [
                Mock(
                    status_code=200,
                    headers={'Content-Type': 'application/json'},
                    json=Mock(return_value=ms_teams_handler_config.TEST_GROUP_DATA)
                ),
                Mock(
                    status_code=200,
                    headers={'Content-Type': 'application/json'},
                    json=Mock(return_value=ms_teams_handler_config.TEST_CHANNEL_ID_DATA)
                ),
                Mock(
                    status_code=200,
                    headers={'Content-Type': 'application/json'},
                    json=Mock(return_value={'value': ms_teams_handler_config.TEST_CHANNEL_MESSAGES_DATA})
                ),
            ]

        # if the group_ids parameter is set, the mocks will only return the channel ID data, then the channel messages data
        else:
            mock_get.side_effect = [
                Mock(
                    status_code=200,
                    headers={'Content-Type': 'application/json'},
                    json=Mock(return_value=ms_teams_handler_config.TEST_CHANNEL_ID_DATA)
                ),
                Mock(
                    status_code=200,
                    headers={'Content-Type': 'application/json'},
                    json=Mock(return_value={'value': ms_teams_handler_config.TEST_CHANNEL_MESSAGES_DATA})
                ),
            ]

        channel_messages_data = self.api_client.get_channel_messages()

        # assert the requests.get calls were made with the expected arguments
        # if the group_ids parameter is not set, check the calls to the groups endpoint
        if not is_group_ids_set:
            mock_get.assert_any_call(
                'https://graph.microsoft.com/v1.0/groups/',
                headers={'Authorization': 'Bearer test_access_token'},
                params={'$select': 'id,resourceProvisioningOptions'}
            )

        mock_get.assert_any_call(
            'https://graph.microsoft.com/v1.0/teams/test_team_id/channels/',
            headers={'Authorization': 'Bearer test_access_token'},
            params=None
        )

        mock_get.assert_any_call(
            'https://graph.microsoft.com/v1.0/teams/test_team_id/channels/test_channel_id/messages/',
            headers={'Authorization': 'Bearer test_access_token'},
            params={'$top': 20}
        )

        self.assertEqual(channel_messages_data[0]["id"], "test_id")
        self.assertEqual(channel_messages_data[0]["messageType"], "message")
        self.assertEqual(channel_messages_data[0]["channelIdentity"]["channelId"], "test_channel_id")
        self.assertEqual(channel_messages_data[0]["channelIdentity"]["teamId"], "test_team_id")

    @patch('requests.post')
    def test_send_channel_message_sends_correct_request(self, mock_post):
        """
        Test that send_channel_message sends a channel message.
        """

        # configure the mock to return a response with 'status_code' 201
        mock_post.return_value = Mock(
            status_code=201,
            headers={'Content-Type': 'application/json'},
        )

        self.api_client.send_channel_message("test_team_id", "test_channel_id", "test_message", "test_subject")

        # assert the requests.post call was made with the expected arguments
        mock_post.assert_called_once_with(
            'https://graph.microsoft.com/v1.0/teams/test_team_id/channels/test_channel_id/messages/',
            headers={'Authorization': 'Bearer test_access_token'},
            json={'subject': 'test_subject', 'body': {'content': 'test_message'}}
        )


class TestChatsTable(unittest.TestCase):
    """
    Tests for the ChatsTable class.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the tests.
        """

        # mock the api handler
        cls.api_handler = Mock(MSTeamsHandler)

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        chats_table = ChatsTable(self.api_handler)

        self.assertListEqual(chats_table.get_columns(), ms_teams_handler_config.CHATS_TABLE_COLUMNS)

    def test_select_star_for_single_chat_returns_all_columns(self):
        # patch the api handler to return the chat data
        with patch.object(self.api_handler.connect(), 'get_chat', return_value=ms_teams_handler_config.TEST_CHAT_DATA):
            chats_table = ChatsTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="chats",
                where=BinaryOperation(
                    op='=',
                    args=[
                        Identifier('id'),
                        Constant("test_id")
                    ]
                )
            )

            all_chats = chats_table.select(select_all)
            first_chat = all_chats.iloc[0]

            self.assertEqual(all_chats.shape[1], len(ms_teams_handler_config.CHATS_TABLE_COLUMNS))
            self.assertEqual(first_chat["id"], "test_id")
            self.assertEqual(first_chat["chatType"], "oneOnOne")

    def test_select_star_for_all_chats_returns_all_columns(self):
        # patch the api handler to return the chat data
        with patch.object(self.api_handler.connect(), 'get_chats', return_value=ms_teams_handler_config.TEST_CHATS_DATA):
            chats_table = ChatsTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="chats",
            )

            all_chats = chats_table.select(select_all)
            first_chat = all_chats.iloc[0]

            self.assertEqual(all_chats.shape[1], len(ms_teams_handler_config.CHATS_TABLE_COLUMNS))
            self.assertEqual(first_chat["id"], "test_id")
            self.assertEqual(first_chat["chatType"], "oneOnOne")

    def test_select_for_single_chat_returns_only_selected_columns(self):
        # patch the api handler to return the chat data
        with patch.object(self.api_handler.connect(), 'get_chat', return_value=ms_teams_handler_config.TEST_CHAT_DATA):
            chats_table = ChatsTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and chatType columns
                targets=[
                    Identifier('id'),
                    Identifier('chatType'),
                ],
                from_table="chats",
                where=BinaryOperation(
                    op='=',
                    args=[
                        Identifier('id'),
                        Constant("test_id")
                    ]
                )
            )

            all_chats = chats_table.select(select_all)
            first_chat = all_chats.iloc[0]

            self.assertEqual(all_chats.shape[1], 2)
            self.assertEqual(first_chat["id"], "test_id")
            self.assertEqual(first_chat["chatType"], "oneOnOne")

    def test_select_for_all_chats_returns_only_selected_columns(self):
        # patch the api handler to return the chat data
        with patch.object(self.api_handler.connect(), 'get_chats', return_value=ms_teams_handler_config.TEST_CHATS_DATA):
            chats_table = ChatsTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[
                    Identifier('id'),
                    Identifier('chatType'),
                ],
                from_table="chats",
            )

            all_chats = chats_table.select(select_all)
            first_chat = all_chats.iloc[0]

            self.assertEqual(all_chats.shape[1], 2)
            self.assertEqual(first_chat["id"], "test_id")
            self.assertEqual(first_chat["chatType"], "oneOnOne")


class TestChatMessagesTable(unittest.TestCase):
    """
    Tests for the ChatMessagesTable class.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the tests.
        """

        # mock the api handler
        cls.api_handler = Mock(MSTeamsHandler)

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        chat_messages_table = ChatMessagesTable(self.api_handler)

        self.assertListEqual(chat_messages_table.get_columns(), ms_teams_handler_config.CHAT_MESSAGES_TABLE_COLUMNS)

    def test_select_star_for_single_chat_returns_all_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_chat_message', return_value=ms_teams_handler_config.TEST_CHAT_MESSAGE_DATA):
            chat_messages_table = ChatMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="chat_messages",
                where=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('id'),
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('chatId'),
                            Constant("test_chat_id")
                        ]
                    )
                ]
            )

            all_chat_messages = chat_messages_table.select(select_all)
            first_chat_message = all_chat_messages.iloc[0]

            self.assertEqual(all_chat_messages.shape[1], len(ms_teams_handler_config.CHAT_MESSAGES_TABLE_COLUMNS))
            self.assertEqual(first_chat_message["id"], "test_id")
            self.assertEqual(first_chat_message["messageType"], "message")

    def test_select_star_for_multiple_chats_returns_all_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_chat_messages', return_value=ms_teams_handler_config.TEST_CHAT_MESSAGES_DATA):
            chat_messages_table = ChatMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="chat_messages",
                where=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('chatId'),
                            Constant("test_chat_id")
                        ]
                    )
                ]
            )

            all_chat_messages = chat_messages_table.select(select_all)
            first_chat_message = all_chat_messages.iloc[0]

            self.assertEqual(all_chat_messages.shape[1], len(ms_teams_handler_config.CHAT_MESSAGES_TABLE_COLUMNS))
            self.assertEqual(first_chat_message["id"], "test_id")
            self.assertEqual(first_chat_message["messageType"], "message")

    def test_select_star_for_all_chats_returns_all_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_all_chat_messages', return_value=ms_teams_handler_config.TEST_CHAT_MESSAGES_DATA):
            chat_messages_table = ChatMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="chat_messages",
            )

            all_chat_messages = chat_messages_table.select(select_all)
            first_chat_message = all_chat_messages.iloc[0]

            self.assertEqual(all_chat_messages.shape[1], len(ms_teams_handler_config.CHAT_MESSAGES_TABLE_COLUMNS))
            self.assertEqual(first_chat_message["id"], "test_id")
            self.assertEqual(first_chat_message["messageType"], "message")

    def test_select_for_single_chat_returns_only_selected_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_chat_message', return_value=ms_teams_handler_config.TEST_CHAT_MESSAGE_DATA):
            chat_messages_table = ChatMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and messageType columns
                targets=[
                    Identifier('id'),
                    Identifier('messageType'),
                ],
                from_table="chat_messages",
                where=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('id'),
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('chatId'),
                            Constant("test_chat_id")
                        ]
                    )
                ]
            )

            all_chat_messages = chat_messages_table.select(select_all)
            first_chat_message = all_chat_messages.iloc[0]

            self.assertEqual(all_chat_messages.shape[1], 2)
            self.assertEqual(first_chat_message["id"], "test_id")
            self.assertEqual(first_chat_message["messageType"], "message")

    def test_select_for_multiple_chats_returns_only_selected_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_chat_messages', return_value=ms_teams_handler_config.TEST_CHAT_MESSAGES_DATA):
            chat_messages_table = ChatMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and messageType columns
                targets=[
                    Identifier('id'),
                    Identifier('messageType'),
                ],
                from_table="chat_messages",
                where=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('chatId'),
                            Constant("test_chat_id")
                        ]
                    )
                ]
            )

            all_chat_messages = chat_messages_table.select(select_all)
            first_chat_message = all_chat_messages.iloc[0]

            self.assertEqual(all_chat_messages.shape[1], 2)
            self.assertEqual(first_chat_message["id"], "test_id")
            self.assertEqual(first_chat_message["messageType"], "message")

    def test_select_for_all_chats_returns_only_selected_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_all_chat_messages', return_value=ms_teams_handler_config.TEST_CHAT_MESSAGES_DATA):
            chat_messages_table = ChatMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and messageType columns
                targets=[
                    Identifier('id'),
                    Identifier('messageType'),
                ],
                from_table="chat_messages",
            )

            all_chat_messages = chat_messages_table.select(select_all)
            first_chat_message = all_chat_messages.iloc[0]

            self.assertEqual(all_chat_messages.shape[1], 2)
            self.assertEqual(first_chat_message["id"], "test_id")
            self.assertEqual(first_chat_message["messageType"], "message")

    def test_insert_chat_message_calls_correct_method_in_client(self):
        """
        Test that send_chat_message sends a chat message.
        """

        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'send_chat_message', return_value=None) as mock_send_chat_message:
            chat_messages_table = ChatMessagesTable(self.api_handler)

            insert = ast.Insert(
                table="chat_messages",
                columns=[
                    Identifier('chatId'),
                    Identifier('body_content'),
                    Identifier('subject'),
                ],
                values=[
                    (
                        "test_chat_id",
                        "test_message",
                        "test_subject"
                    )
                ]
            )

            chat_messages_table.insert(insert)

            # assert the api handler's send_chat_message method was called with the expected arguments
            mock_send_chat_message.assert_called_once_with(chat_id='test_chat_id', message='test_message', subject='test_subject')


class TestChannelsTable(unittest.TestCase):
    """
    Tests for the ChannelsTable class.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the tests.
        """

        # mock the api handler
        cls.api_handler = Mock(MSTeamsHandler)

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        channels_table = ChannelsTable(self.api_handler)

        self.assertListEqual(channels_table.get_columns(), ms_teams_handler_config.CHANNELS_TABLE_COLUMNS)

    def test_select_star_for_single_channel_returns_all_columns(self):
        # patch the api handler to return the channel data
        with patch.object(self.api_handler.connect(), 'get_channel', return_value=ms_teams_handler_config.TEST_CHANNEL_DATA):
            channels_table = ChannelsTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="channels",
                where=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('id'),
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('teamId'),
                            Constant("test_team_id")
                        ]
                    )
                ]
            )

            all_channels = channels_table.select(select_all)
            first_channel = all_channels.iloc[0]

            self.assertEqual(all_channels.shape[1], len(ms_teams_handler_config.CHANNELS_TABLE_COLUMNS))
            self.assertEqual(first_channel["id"], "test_id")
            self.assertEqual(first_channel["displayName"], "test_display_name")

    def test_select_star_for_all_channels_returns_all_columns(self):
        # patch the api handler to return the channel data
        with patch.object(self.api_handler.connect(), 'get_channels', return_value=ms_teams_handler_config.TEST_CHANNELS_DATA):
            channels_table = ChannelsTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="channels",
            )

            all_channels = channels_table.select(select_all)
            first_channel = all_channels.iloc[0]

            self.assertEqual(all_channels.shape[1], len(ms_teams_handler_config.CHANNELS_TABLE_COLUMNS))
            self.assertEqual(first_channel["id"], "test_id")
            self.assertEqual(first_channel["displayName"], "test_display_name")

    def test_select_for_single_channel_returns_only_selected_columns(self):
        # patch the api handler to return the channel data
        with patch.object(self.api_handler.connect(), 'get_channel', return_value=ms_teams_handler_config.TEST_CHANNEL_DATA):
            channels_table = ChannelsTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and displayName columns
                targets=[
                    Identifier('id'),
                    Identifier('displayName'),
                ],
                from_table="channels",
                where=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('id'),
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('teamId'),
                            Constant("test_team_id")
                        ]
                    )
                ]
            )

            all_channels = channels_table.select(select_all)
            first_channel = all_channels.iloc[0]

            self.assertEqual(all_channels.shape[1], 2)
            self.assertEqual(first_channel["id"], "test_id")
            self.assertEqual(first_channel["displayName"], "test_display_name")

    def test_select_for_all_channels_returns_only_selected_columns(self):
        # patch the api handler to return the channel data
        with patch.object(self.api_handler.connect(), 'get_channels', return_value=ms_teams_handler_config.TEST_CHANNELS_DATA):
            channels_table = ChannelsTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and displayName columns
                targets=[
                    Identifier('id'),
                    Identifier('displayName'),
                ],
                from_table="channels",
            )

            all_channels = channels_table.select(select_all)
            first_channel = all_channels.iloc[0]

            self.assertEqual(all_channels.shape[1], 2)
            self.assertEqual(first_channel["id"], "test_id")
            self.assertEqual(first_channel["displayName"], "test_display_name")


class TestChannelMessagesTable(unittest.TestCase):
    """
    Tests for the ChannelMessagesTable class.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the tests.
        """

        # mock the api handler
        cls.api_handler = Mock(MSTeamsHandler)

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        channel_messages_table = ChannelMessagesTable(self.api_handler)

        self.assertListEqual(channel_messages_table.get_columns(), ms_teams_handler_config.CHANNEL_MESSAGES_TABLE_COLUMNS)

    def test_select_star_for_single_channel_message_returns_all_columns(self):
        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'get_channel_message', return_value=ms_teams_handler_config.TEST_CHANNEL_MESSAGE_DATA):
            channel_messages_table = ChannelMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="channel_messages",
                where=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('id'),
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('channelIdentity_channelId'),
                            Constant("test_channel_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('channelIdentity_teamId'),
                            Constant("test_team_id")
                        ]
                    )
                ]
            )

            all_channel_messages = channel_messages_table.select(select_all)
            first_channel_message = all_channel_messages.iloc[0]

            self.assertEqual(all_channel_messages.shape[1], len(ms_teams_handler_config.CHANNEL_MESSAGES_TABLE_COLUMNS))
            self.assertEqual(first_channel_message["id"], "test_id")
            self.assertEqual(first_channel_message["messageType"], "message")

    def test_select_star_for_all_channel_messages_returns_all_columns(self):
        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'get_channel_messages', return_value=ms_teams_handler_config.TEST_CHANNEL_MESSAGES_DATA):
            channel_messages_table = ChannelMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="channel_messages",
            )

            all_channel_messages = channel_messages_table.select(select_all)
            first_channel_message = all_channel_messages.iloc[0]

            self.assertEqual(all_channel_messages.shape[1], len(ms_teams_handler_config.CHANNEL_MESSAGES_TABLE_COLUMNS))
            self.assertEqual(first_channel_message["id"], "test_id")
            self.assertEqual(first_channel_message["messageType"], "message")

    def test_select_for_single_channel_message_returns_only_selected_columns(self):
        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'get_channel_message', return_value=ms_teams_handler_config.TEST_CHANNEL_MESSAGE_DATA):
            channel_messages_table = ChannelMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and messageType columns
                targets=[
                    Identifier('id'),
                    Identifier('messageType'),
                ],
                from_table="channel_messages",
                where=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('id'),
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('channelIdentity_channelId'),
                            Constant("test_channel_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('channelIdentity_teamId'),
                            Constant("test_team_id")
                        ]
                    )
                ]
            )

            all_channel_messages = channel_messages_table.select(select_all)
            first_channel_message = all_channel_messages.iloc[0]

            self.assertEqual(all_channel_messages.shape[1], 2)
            self.assertEqual(first_channel_message["id"], "test_id")
            self.assertEqual(first_channel_message["messageType"], "message")

    def test_select_for_all_channel_messages_returns_only_selected_columns(self):
        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'get_channel_messages', return_value=ms_teams_handler_config.TEST_CHANNEL_MESSAGES_DATA):
            channel_messages_table = ChannelMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and messageType columns
                targets=[
                    Identifier('id'),
                    Identifier('messageType'),
                ],
                from_table="channel_messages",
            )

            all_channel_messages = channel_messages_table.select(select_all)
            first_channel_message = all_channel_messages.iloc[0]

            self.assertEqual(all_channel_messages.shape[1], 2)
            self.assertEqual(first_channel_message["id"], "test_id")
            self.assertEqual(first_channel_message["messageType"], "message")

    def test_insert_channel_message_calls_correct_method_in_client(self):
        """
        Test that insert_channel_message calls the correct method in the client.
        """

        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'send_channel_message', return_value=None) as mock_send_channel_message:
            channel_messages_table = ChannelMessagesTable(self.api_handler)

            insert = ast.Insert(
                table="channel_messages",
                columns=[
                    Identifier('channelIdentity_teamId'),
                    Identifier('channelIdentity_channelId'),
                    Identifier('body_content'),
                    Identifier('subject'),
                ],
                values=[
                    (
                        "test_team_id",
                        "test_channel_id",
                        "test_message",
                        "test_subject"
                    )
                ]
            )

            channel_messages_table.insert(insert)

            # assert the api handler's send_channel_message method was called with the expected arguments
            mock_send_channel_message.assert_called_once_with(group_id='test_team_id', channel_id='test_channel_id', message='test_message', subject='test_subject')


if __name__ == "__main__":
    unittest.main()
