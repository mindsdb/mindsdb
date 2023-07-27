import requests

class ChatbotAlerter():

    def send_slack_alert(self, hook_service_url, text, attachments):
        slack_message = {
          "text": text
        }

        if attachments:
            slack_message["attachments"] = attachments

        requests.post(hook_service_url, json=slack_message)
