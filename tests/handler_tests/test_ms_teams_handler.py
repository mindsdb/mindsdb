import unittest
from unittest.mock import Mock, patch, MagicMock

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import Constant, BinaryOperation

from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_handler import MSTeamsHandler
from mindsdb.integrations.handlers.ms_teams_handler.settings import ms_teams_handler_config
from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_tables import ChatsTable, ChatMessagesTable, ChannelsTable, ChannelMessagesTable


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

        # define the chat data to return
        cls.chat_data = {
            '@odata.context': 'test_context',
            'id': 'test_id',
            'topic': None,
            'createdDateTime': '2023-11-20T10:25:19.553Z',
            'lastUpdatedDateTime': '2023-11-20T10:25:19.553Z',
            'chatType': 'oneOnOne',
            'webUrl': 'https://teams.test',
            'tenantId': 'test_tenant_id',
            'onlineMeetingInfo': None,
            'viewpoint': {
                'isHidden': False,
                'lastMessageReadDateTime': '2023-12-08T17:09:34.214Z'
            },
            'lastMessagePreview@odata.context': 'https://graph.test',
            'lastMessagePreview': {
                'id': '1702055374214',
                'createdDateTime': '2023-12-08T17:09:34.214Z',
                'isDeleted': False,
                'messageType': 'message',
                'eventDetail': None,
                'body': {
                    'contentType': 'text',
                    'content': '\n\nTest message.'
                },
                'from': {
                    'application': None,
                    'device': None,
                    'user': {}
                }
            }
        }

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        chats_table = ChatsTable(self.api_handler)

        self.assertListEqual(chats_table.get_columns(), ms_teams_handler_config.CHATS_TABLE_COLUMNS)

    def test_select_star_returns_all_columns(self):
        # patch the api handler to return the chat data
        with patch.object(self.api_handler.connect(), 'get_chat', return_value=self.chat_data):
            chats_table = ChatsTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="chats",
                where=BinaryOperation(op='=',
                    args = [
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

    def test_select_returns_only_selected_columns(self):
        # patch the api handler to return the chat data
        with patch.object(self.api_handler.connect(), 'get_chat', return_value=self.chat_data):
            chats_table = ChatsTable(self.api_handler)

            select_all = ast.Select(
                # select only the id and chatType columns
                targets=[
                    Identifier('id'),
                    Identifier('chatType'),
                ],
                from_table="chats",
                where=BinaryOperation(op='=',
                    args = [
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

        # define the chat message data to return
        cls.chat_message_data = {
            '@odata.context': 'test_context', 
            'id': 'test_id', 
            'replyToId': None, 
            'etag': 'test_etag', 
            'messageType': 'message', 
            'createdDateTime': '2023-12-08T17:09:22.241Z', 
            'lastModifiedDateTime': '2023-12-08T17:09:22.241Z', 
            'lastEditedDateTime': None, 
            'deletedDateTime': None, 
            'subject': None, 
            'summary': None, 
            'chatId': 'test_chat_id', 
            'importance': 'normal', 
            'locale': 'en-us',
            'webUrl': None,
            'channelIdentity': None,
            'policyViolation': None,
            'attachments': [],
            'mentions': [],
            'reactions': [],
            'from': {
                'application': None,
                'device': None,
                'user': {
                    '@odata.type': 'test_type',
                    'id': 'test_user_id',
                    'displayName': 'test_user_display_name',
                    'userIdentityType': 'aadUser',
                    'tenantId': 'test_tenant_id'
                }
            },
            'body': {
                'contentType': 'text',
                'content': '\n\nTest message.'
            }
        }

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        chat_messages_table = ChatMessagesTable(self.api_handler)

        self.assertListEqual(chat_messages_table.get_columns(), ms_teams_handler_config.CHAT_MESSAGES_TABLE_COLUMNS)

    def test_select_star_returns_all_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_chat_message', return_value=self.chat_message_data):
            chat_messages_table = ChatMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="chat_messages",
                where=[
                    BinaryOperation(
                        op='=',
                        args = [
                            Identifier('id'), 
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args = [
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

    def test_select_returns_only_selected_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_chat_message', return_value=self.chat_message_data):
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
                        args = [
                            Identifier('id'), 
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args = [
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

        # define the channel data to return
        cls.channel_data = {
            '@odata.context': 'test_context', 
            'id': 'test_id', 
            'createdDateTime': '2023-11-17T22:54:33.055Z', 
            'displayName': 'test_display_name', 
            'description': None, 
            'isFavoriteByDefault': None, 
            'email': 'test@test.com', 
            'tenantId': 'test_tenant_id', 
            'webUrl': 'https://teams.test', 
            'membershipType': 'standard', 
            'teamId': 'test_team_id'
        }

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        channels_table = ChannelsTable(self.api_handler)

        self.assertListEqual(channels_table.get_columns(), ms_teams_handler_config.CHANNELS_TABLE_COLUMNS)

    def test_select_star_returns_all_columns(self):
        # patch the api handler to return the channel data
        with patch.object(self.api_handler.connect(), 'get_channel', return_value=self.channel_data):
            channels_table = ChannelsTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="channels",
                where=[
                    BinaryOperation(
                        op='=',
                        args = [
                            Identifier('id'), 
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args = [
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

    def test_select_returns_only_selected_columns(self):
        # patch the api handler to return the channel data
        with patch.object(self.api_handler.connect(), 'get_channel', return_value=self.channel_data):
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
                        args = [
                            Identifier('id'), 
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args = [
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

        # define the channel message data to return
        cls.channel_message_data = {
            '@odata.context': 'test_context', 
            'id': 'test_id', 
            'replyToId': None, 
            'etag': 'test_etag', 
            'messageType': 'message', 
            'createdDateTime': '2023-11-30T16:52:50.18Z', 
            'lastModifiedDateTime': '2023-11-30T16:52:50.18Z', 
            'lastEditedDateTime': None, 
            'deletedDateTime': None, 
            'subject': 'Test Subject', 
            'summary': None, 
            'chatId': None, 
            'importance': 
            'normal', 
            'locale': 'en-us',
            'webUrl': 'https://teams.test',
            'policyViolation': None,
            'attachments': [],
            'mentions': [],
            'reactions': [],
            'from': {
                'application': None,
                'device': None,
                'user': {
                    '@odata.type': 'test_type',
                    'id': 'test_user_id',
                    'displayName': 'test_user_display_name',
                    'userIdentityType': 'aadUser',
                    'tenantId': 'test_tenant_id'
                }
            },
            'body': {
                'contentType': 'text',
                'content': '\n\nTest message.'
            },
            'channelIdentity': {
                'teamId': 'test_team_id',
                'channelId': 'test_channel_id'
            },
            'eventDetail': {
                '@odata.type': 'test_type',
                'visibleHistoryStartDateTime': '2023-11-30T16:52:50.18Z',
                'members': [],
                'initiator': {
                    'device': None,
                    'application': {
                        '@odata.type': 'test_type',
                        'id': 'test_app_id',
                        'displayName': 'test_app_display_name',
                        'applicationIdentityType': 'bot'
                    },
                    'user': {
                        '@odata.type': 'test_type',
                        'id': 'test_user_id',
                        'displayName': 'test_user_display_name',
                        'userIdentityType': 'aadUser',
                        'tenantId': 'test_tenant_id'
                    }
                },
                'channelId': 'test_channel_id',
                'channelDisplayName': 'test_channel_display_name'
            }
        }

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        channel_messages_table = ChannelMessagesTable(self.api_handler)

        self.assertListEqual(channel_messages_table.get_columns(), ms_teams_handler_config.CHANNEL_MESSAGES_TABLE_COLUMNS)

    def test_select_star_returns_all_columns(self):
        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'get_channel_message', return_value=self.channel_message_data):
            channel_messages_table = ChannelMessagesTable(self.api_handler)

            select_all = ast.Select(
                # select all columns
                targets=[Star()],
                from_table="channel_messages",
                where=[
                    BinaryOperation(
                        op='=',
                        args = [
                            Identifier('id'), 
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args = [
                            Identifier('channelIdentity_channelId'), 
                            Constant("test_channel_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args = [
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

    def test_select_returns_only_selected_columns(self):
        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'get_channel_message', return_value=self.channel_message_data):
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
                        args = [
                            Identifier('id'), 
                            Constant("test_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args = [
                            Identifier('channelIdentity_channelId'), 
                            Constant("test_channel_id")
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args = [
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


if __name__ == "__main__":
    unittest.main()
