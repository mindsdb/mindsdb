from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.webhooks import ns_conf
from mindsdb.interfaces.chatbot.chatbot_controller import ChatBotController
from mindsdb.metrics.metrics import api_endpoint_metrics


@ns_conf.route('/chatbots/<webhook_token>')
class ChatbotWebhooks(Resource):
    @ns_conf.doc('chatbots_webhook')
    @api_endpoint_metrics('POST', '/chatbots/<webhook_token>')
    def post(self, webhook_token):
        """
        This endpoint is used to receive messages posted by bots from different platforms.

        :param webhook_token: The token of the webhook. It is used to uniquely identify the webhook.        
        """
        chatbot_controller = ChatBotController()
        return chatbot_controller.on_webhook(webhook_token)