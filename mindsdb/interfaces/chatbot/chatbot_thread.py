import threading
from mindsdb.utilities.context import context as ctx

from .chatbot_task import ChatBotTask


class ChatBotThread(threading.Thread):
    """A thread for polling style chatbots to operate."""
    def __init__(self, bot_record):
        threading.Thread.__init__(self)
        self.bot_record = bot_record
        self.chatbot_task = None

    def run(self):
        # create context and session

        ctx.set_default()
        ctx.company_id = self.bot_record.company_id
        if self.bot_record.user_class is not None:
            ctx.user_class = self.bot_record.user_class

        self.chatbot_task = ChatBotTask(self.bot_record)

    def stop(self):

        self.chatbot_task.stop()
