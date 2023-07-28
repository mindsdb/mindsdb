import threading

from mindsdb.utilities import log
from mindsdb.interfaces.chatbot.realtime_chatbot_task import RealtimeChatBotTask
from mindsdb.interfaces.chatbot.realtime_chat_handler_factory import RealtimeChatHandlerFactory

class RealtimeChatBotThread(threading.Thread):
    """A thread for a realtime chatbot to operate."""

    def __init__(self, alerter, bot_record):
        threading.Thread.__init__(self, daemon=True)
        self._bot_record = bot_record
        self._chatbot_task = None
        self.alerter= alerter

    def run(self):
        """Starts running the chatbot"""
        self._chatbot_task = RealtimeChatBotTask(
            RealtimeChatHandlerFactory(),
            alerter= self.alerter,
            chat_engine=self._bot_record.chat_engine,
            bot_record=self._bot_record)

        try:
            self._chatbot_task.run() 
        except Exception as e:
            self.alerter.send_slack_alert(
                "@here :robot_face: : The chatbot is unable to establish a connection",
                [
                    {
                    "color": "#C80001",
                    "fields": [
                        {
                            "title": "Chatbot id",
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
