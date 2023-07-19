import datetime as dt

from mindsdb.integrations.libs.api_handler import APIChatHandler

from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.interfaces.storage import db

from .polling import MessageCountPolling, RealtimePolling
from .memory import DBMemory, HandlerMemory
from .chatbot_executor import MultiModeBotExecutor, BotExecutor

from .types import ChatBotMessage


class ChatBotTask:

    """A thread for polling style chatbots to operate."""

    def __init__(self, bot_id):
        self.bot_id = bot_id

    def run(self, stop_event):

        bot_record = db.ChatBots.query.get(self.bot_id)

        session = SessionController()
        # TODO check deleted, raise errors
        # TODO checks on delete predictor/ project/ integration

        self.base_model_name = bot_record.model_name
        self.project_name = db.Project.query.get(bot_record.project_id).name
        self.project_datanode = session.datahub.get(self.project_name)

        database_name = db.Integration.query.get(bot_record.database_id).name

        self.chat_handler = session.integration_controller.get_handler(database_name)
        if not isinstance(self.chat_handler, APIChatHandler):
            raise Exception(f"Can't use chat database: {database_name}")

        # get chat handler info
        self.bot_params = bot_record.params or {}

        chat_params = self.chat_handler.get_chat_config()
        self.bot_params['bot_username'] = self.chat_handler.get_my_user_name()

        polling = chat_params['polling']
        if polling == 'message_count':
            self.chat_pooling = MessageCountPolling(self, chat_params)
            self.memory = HandlerMemory(self, chat_params)

        elif polling == 'realtime':
            self.chat_pooling = RealtimePolling(self, chat_params)
            self.memory = DBMemory(self, chat_params)
        else:
            raise Exception(f"Not supported polling: {polling}")

        if self.bot_params.get('modes') is None:
            self.bot_executor_cls = BotExecutor
        else:
            self.bot_executor_cls = MultiModeBotExecutor

        self.model_manager = ...

        self.chat_pooling.run(stop_event)

    def on_message(self, chat_memory, message: ChatBotMessage):

        # add question to history
        # TODO move it to realtime pooling
        chat_memory.add_to_history(message)

        history = chat_memory.get_history(cached=False)

        # process
        bot_executor = self.bot_executor_cls(self, chat_memory)
        response_text = bot_executor.process()

        chat_id = message.destination
        bot_username = self.bot_params['bot_username']
        response_message = ChatBotMessage(
            ChatBotMessage.Type.DIRECT,
            response_text,
            # In Slack direct messages are treated as channels themselves.
            user=bot_username,
            destination=chat_id,
            sent_at=dt.datetime.now()
        )

        # send to chat adapter
        self.chat_pooling.send_message(response_message)

        # send to history
        history.add_to_history(response_message)
