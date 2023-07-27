import threading

from mindsdb.utilities import log
from mindsdb.interfaces.chatbot.realtime_chatbot_task import RealtimeChatBotTask
from mindsdb.interfaces.chatbot.realtime_chat_handler_factory import RealtimeChatHandlerFactory
from mindsdb.interfaces.chatbot.chatbot_alerter import ChatbotAlerter

class RealtimeChatBotThread(threading.Thread):
    """A thread for a realtime chatbot to operate."""

    def __init__(self, bot_record):
        threading.Thread.__init__(self, daemon=True)
        self._bot_record = bot_record
        self._chatbot_task = None

    def run(self):
        """Starts running the chatbot"""
        self._chatbot_task = RealtimeChatBotTask(
            RealtimeChatHandlerFactory(),
            chat_engine=self._bot_record.chat_engine,
            bot_record=self._bot_record)

        try:
            self._chatbot_task.run() 
        except Exception as e:
            ChatbotAlerter.send_slack_alert(
                self,
                'https://hooks.slack.com/services/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',
                "@here :robot_face: : The chatbot is unable to establish a connection",
                [
                    {
                    "color": "#C80001",
                    "fields": [
                        {
                            "title": "chatbot id",
                            "value": self._bot_record.id
                        }
                    ],
                    }
                ]
            )

            log.logger.error(e)

    def stop(self):
        """Stops running the chatbot"""
        if self._chatbot_task:
            self._chatbot_task.stop()
