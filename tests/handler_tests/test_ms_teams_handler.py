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

    def test_select_star_returns_correct_data(self):
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

        cls.api_handler = Mock(MSTeamsHandler)

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        chat_messages_table = ChatMessagesTable(self.api_handler)

        self.assertListEqual(chat_messages_table.get_columns(), ms_teams_handler_config.CHAT_MESSAGES_TABLE_COLUMNS)


class TestChannelsTable(unittest.TestCase):
    """
    Tests for the ChannelsTable class.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the tests.
        """

        cls.api_handler = Mock(MSTeamsHandler)

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        channels_table = ChannelsTable(self.api_handler)

        self.assertListEqual(channels_table.get_columns(), ms_teams_handler_config.CHANNELS_TABLE_COLUMNS)


class TestChannelMessagesTable(unittest.TestCase):
    """
    Tests for the ChannelMessagesTable class.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the tests.
        """

        cls.api_handler = Mock(MSTeamsHandler)

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """

        channel_messages_table = ChannelMessagesTable(self.api_handler)

        self.assertListEqual(channel_messages_table.get_columns(), ms_teams_handler_config.CHANNEL_MESSAGES_TABLE_COLUMNS)


if __name__ == "__main__":
    unittest.main()
