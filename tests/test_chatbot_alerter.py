import unittest
from unittest.mock import patch
from mindsdb.interfaces.chatbot.chatbot_alerter import ChatbotAlerter

class TestChatbotAlerter(unittest.TestCase):
    """
      Test case class for testing the ChatbotAlerter class.

      Methods:
          setUp(): A method that runs before each test method to set up the test environment.
          test_send_slack_alert(): A test method to test the send_slack_alert method with attachments.
          test_send_slack_alert_without_attachments(): A test method to test the send_slack_alert method without attachments.
     """

    def setUp(self):
        """
          Set up the test environment before each test method.
        """
        self.webhook_url = "https://hooks.slack.com/services/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"
        self.bot_alerter = ChatbotAlerter(self.webhook_url)

    def test_send_slack_alert(self):
        """
          Test the send_slack_alert method with attachments.

          This method uses unittest.mock.patch to mock the 'requests.post' method
          and ensure that it was called with the correct arguments.
        """
        message_text = "Hello, this is a test message."
        attachments = {"title": "Test Attachment", "text": "This is an attachment."}

        with patch('requests.post') as mock_post:
            self.bot_alerter.send_slack_alert(message_text, attachments)

            # Ensure that requests.post was called with the correct arguments
            mock_post.assert_called_once_with(
                self.webhook_url,
                json={"text": message_text, "attachments": attachments}
            )

if __name__ == "__main__":
    unittest.main()
