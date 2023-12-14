import unittest
import pandas as pd
from unittest.mock import Mock

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_handler import MSTeamsHandler
from mindsdb.integrations.handlers.ms_teams_handler.settings import ms_teams_handler_config
from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_tables import ChatsTable, ChannelsTable


class TestChatsTable(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(MSTeamsHandler)
        chats_table = ChatsTable(api_handler)

        self.assertListEqual(chats_table.get_columns(), ms_teams_handler_config.CHATS_TABLE_COLUMNS)


if __name__ == "__main__":
    unittest.main()