import unittest
from unittest.mock import patch

from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast import BinaryOperation, Identifier, Constant
from mindsdb_sql_parser.ast.select.star import Star
from mindsdb.integrations.handlers.discord_handler.discord_handler import DiscordHandler


class DiscordHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.handler = DiscordHandler(
            name='discord_datasource', connection_data={'token': 'test-discord-token'}
        )

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    @patch('mindsdb.integrations.handlers.discord_handler.discord_handler.requests.get')
    def test_1_read_messages(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{'content': 'Test message'}]
        query = ast.Select(
            targets=[Star()],
            from_table="messages",
            where=BinaryOperation(
                op='=', args=[Identifier('channel_id'), Constant('1234567890')]
            ),
        )

        self.handler._tables['messages'].select(query)
        mock_get.assert_called_with(
            'https://discord.com/api/v10/channels/1234567890/messages',
            headers={
                'Authorization': 'Bot test-discord-token',
                'Content-Type': 'application/json',
            },
            params={'limit': 100},
        )

    @patch(
        'mindsdb.integrations.handlers.discord_handler.discord_handler.requests.post'
    )
    def test_2_send_message(self, mock_post):
        mock_post.return_value.status_code = 200
        self.handler._tables['messages'].send_message(
            [{'channel_id': '1234567890', 'text': 'Test message'}]
        )
        mock_post.assert_called_with(
            'https://discord.com/api/v10/channels/1234567890/messages',
            headers={
                'Authorization': 'Bot test-discord-token',
                'Content-Type': 'application/json',
            },
            json={'content': 'Test message'},
        )


if __name__ == '__main__':
    unittest.main()
