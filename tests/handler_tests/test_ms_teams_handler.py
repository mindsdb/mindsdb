import unittest
from unittest.mock import Mock, patch

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

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        chats_table = ChatsTable(self.api_handler)

        self.assertListEqual(chats_table.get_columns(), ms_teams_handler_config.CHATS_TABLE_COLUMNS)

    def test_select_star_returns_all_columns(self):
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

    def test_select_returns_only_selected_columns(self):
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

    def test_select_star_returns_all_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_chat_message', return_value=ms_teams_handler_config.TEST_CHAT_MESSAGES_DATA):
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

    def test_select_returns_only_selected_columns(self):
        # patch the api handler to return the chat message data
        with patch.object(self.api_handler.connect(), 'get_chat_message', return_value=ms_teams_handler_config.TEST_CHAT_MESSAGES_DATA):
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

    def test_select_star_returns_all_columns(self):
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

    def test_select_returns_only_selected_columns(self):
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

    def test_select_star_returns_all_columns(self):
        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'get_channel_message', return_value=ms_teams_handler_config.TEST_CHANNEL_MESSAGES_DATA):
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

    def test_select_returns_only_selected_columns(self):
        # patch the api handler to return the channel message data
        with patch.object(self.api_handler.connect(), 'get_channel_message', return_value=ms_teams_handler_config.TEST_CHANNEL_MESSAGES_DATA):
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


if __name__ == "__main__":
    unittest.main()
