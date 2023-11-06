from mindsdb.integrations.handlers.rocket_chat_handler.rocket_chat_tables import (
    RocketChatMessagesTable,
)
from mindsdb.integrations.handlers.rocket_chat_handler.rocket_chat_handler import (
    RocketChatHandler,
)
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

from unittest.mock import Mock

import pandas as pd
import unittest


class RocketChatMessagesTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(RocketChatHandler)
        messages_table = RocketChatMessagesTable(api_handler)
        # Order matters.
        expected_columns = [
            "id",
            "room_id",
            "bot_id",
            "text",
            "username",
            "name",
            "sent_at",
        ]
        self.assertListEqual(messages_table.get_columns(), expected_columns)

    def test_select_returns_all_columns(self):
        api_handler = Mock(RocketChatHandler)
        api_handler.call_rocket_chat_api.return_value = pd.DataFrame(
            [
                [
                    "message_id_1",  # id
                    "GENERAL",  # room_id
                    "bot_id_1",  # bot_id
                    "YEWWWWW",  # text
                    "shoresey",  # username
                    "Shore Keeso",  # name
                    "2023-05-05T00:31:46.825Z",  # sent_at
                ]
            ]
        )
        messages_table = RocketChatMessagesTable(api_handler)

        select_all = ast.Select(
            targets=[Star()], from_table="channel_messages", where='room_id = "GENERAL"'
        )

        all_messages = messages_table.select(select_all)
        first_message = all_messages.iloc[0]

        self.assertEqual(all_messages.shape[1], 7)
        self.assertEqual(first_message["id"], "message_id_1")
        self.assertEqual(first_message["room_id"], "GENERAL")
        self.assertEqual(first_message["bot_id"], "bot_id_1")
        self.assertEqual(first_message["text"], "YEWWWWW")
        self.assertEqual(first_message["username"], "shoresey")
        self.assertEqual(first_message["name"], "Shore Keeso")
        self.assertEqual(first_message["sent_at"], "2023-05-05T00:31:46.825Z")

    def test_select_returns_only_selected_columns(self):
        api_handler = Mock(RocketChatHandler)
        api_handler.call_rocket_chat_api.return_value = pd.DataFrame(
            [
                [
                    "message_id_1",  # id
                    "GENERAL",  # room_id
                    "bot_id_1",  # bot_id
                    "YEWWWWW",  # text
                    "shoresey",  # username
                    "Shore Keeso",  # name
                    "2023-05-05T00:31:46.825Z",  # sent_at
                ]
            ]
        )
        messages_table = RocketChatMessagesTable(api_handler)

        room_id_identifier = Identifier(path_str="room_id")
        text_identifier = Identifier(path_str="text")
        select_basic = ast.Select(
            targets=[room_id_identifier, text_identifier],
            from_table="channel_messages",
            where='room_id = "GENERAL"',
        )

        all_messages = messages_table.select(select_basic)
        first_message = all_messages.iloc[0]

        self.assertEqual(all_messages.shape[1], 2)
        self.assertEqual(first_message["room_id"], "GENERAL")
        self.assertEqual(first_message["text"], "YEWWWWW")
