from flask import request
from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.webhooks import ns_conf
from mindsdb.interfaces.chatbot.chatbot_controller import ChatBotController
from mindsdb.metrics.metrics import api_endpoint_metrics


# Stores the memory of the various chat-bots mapped by their webhook tokens.
# This is required because each time a new request is made, a new instance of the ChatBotTask is created.
# This causes the memory to be lost.
chat_bot_memory = {}


@ns_conf.route('/chatbots/<webhook_token>')
class ChatbotWebhooks(Resource):
    @ns_conf.doc('chatbots_webhook')
    @api_endpoint_metrics('POST', '/webhooks/chatbots/<webhook_token>')
    def post(self, webhook_token: str) -> None:
        """
        This endpoint is used to receive messages posted by bots from different platforms.

        Args:
        webhook_token (str): The token of the webhook. It is used to uniquely identify the webhook.
        """
        request_data = request.json

        chat_bot_controller = ChatBotController()
        return chat_bot_controller.on_webhook(webhook_token, request_data, chat_bot_memory)
