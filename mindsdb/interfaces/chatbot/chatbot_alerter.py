import requests

class ChatbotAlerter():
    """
      A class representing a Chatbot Alerter that sends Slack alerts.

      Parameters:
        hook_url (str): The URL of the Slack webhook to send the alerts
    """

    def __init__(self, hook_url):
      self.hook_url = hook_url

    def send_slack_alert(self, text, attachments):
        """
          Sends a Slack alert with the provided text and optional attachments.

          Parameters:
            text (str): The main text content of the Slack message.
            attachments (dict or None): Optional. A dictionary containing additional
            information to attach to the Slack message. Defaults to None.

          Returns:
            None
        """
        slack_message = {
          "text": text
        }

        if attachments:
            slack_message["attachments"] = attachments

        requests.post(self.hook_url, json=slack_message)
