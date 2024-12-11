from mindsdb.integrations.handlers.slack_handler import SlackHandler
import unittest
from unittest.mock import patch


class SlackHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = SlackHandler('test-slack-token')

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    @patch('mindsdb.integrations.handlers.slack_handler.slack_handler.requests.post')
    def test_1_send_message(self, mock_post):
        mock_post.return_value.status_code = 200
        self.handler.send_message("#test-channel", "Test message")
        mock_post.assert_called_with(
            'https://slack.com/api/chat.postMessage',
            headers={'Authorization': 'Bearer test-slack-token'},
            json={'channel': '#test-channel', 'text': 'Test message'}
        )


if __name__ == '__main__':
    unittest.main()
