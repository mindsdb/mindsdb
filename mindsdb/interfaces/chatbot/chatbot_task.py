import traceback
import datetime as dt

from mindsdb.integrations.libs.api_handler import APIChatHandler

from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.tasks.task import BaseTask

from mindsdb.utilities import log

from .polling import MessageCountPolling, RealtimePolling
from .memory import DBMemory, HandlerMemory
from .chatbot_executor import MultiModeBotExecutor, BotExecutor

from .types import ChatBotMessage


class ChatBotTask(BaseTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot_id = self.object_id

        self.session = SessionController()

    def run(self, stop_event):

        # TODO check deleted, raise errors
        # TODO checks on delete predictor / project/ integration

        bot_record = db.ChatBots.query.get(self.bot_id)

        self.base_model_name = bot_record.model_name
        self.project_name = db.Project.query.get(bot_record.project_id).name
        self.project_datanode = self.session.datahub.get(self.project_name)

        database_name = db.Integration.query.get(bot_record.database_id).name

        self.chat_handler = self.session.integration_controller.get_handler(database_name)
        if not isinstance(self.chat_handler, APIChatHandler):
            raise Exception(f"Can't use chat database: {database_name}")

        # get chat handler info
        self.bot_params = bot_record.params or {}

        chat_params = self.chat_handler.get_chat_config()
        self.bot_params['bot_username'] = self.chat_handler.get_my_user_name()

        polling = chat_params['polling']['type']
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

        self.chat_pooling.run(stop_event)

    def on_message(self, chat_memory, message: ChatBotMessage):

        try:
            self._on_message(chat_memory, message)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            self.set_error(str(traceback.format_exc()))

    def _on_message(self, chat_memory, message: ChatBotMessage):
        # add question to history
        # TODO move it to realtime pooling
        chat_memory.add_to_history(message)

        log.logger.debug(f'>>chatbot {chat_memory.chat_id} in: {message.text}')

        # process
        bot_executor = self.bot_executor_cls(self, chat_memory)
        response_text = bot_executor.process()

        chat_id = chat_memory.chat_id
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
        log.logger.debug(f'>>chatbot {chat_id} out: {response_message.text}')

        # send to history
        chat_memory.add_to_history(response_message)
