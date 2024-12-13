import traceback
import datetime as dt

from mindsdb.integrations.libs.api_handler import APIChatHandler

from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.tasks.task import BaseTask

from mindsdb.utilities import log

from .polling import MessageCountPolling, RealtimePolling, WebhookPolling
from .memory import BaseMemory, DBMemory, HandlerMemory
from .chatbot_executor import MultiModeBotExecutor, BotExecutor, AgentExecutor

from .types import ChatBotMessage

logger = log.getLogger(__name__)

HOLDING_MESSAGE = "Bot is typing..."


class ChatBotTask(BaseTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot_id = self.object_id

        self.session = SessionController()

        bot_record = db.ChatBots.query.get(self.bot_id)

        self.base_model_name = bot_record.model_name
        self.project_name = db.Project.query.get(bot_record.project_id).name
        self.project_datanode = self.session.datahub.get(self.project_name)

        # get chat handler info
        self.bot_params = bot_record.params or {}

        self.agent_id = bot_record.agent_id
        if self.agent_id is not None:
            self.bot_executor_cls = AgentExecutor
        elif self.bot_params.get('modes') is None:
            self.bot_executor_cls = BotExecutor
        else:
            self.bot_executor_cls = MultiModeBotExecutor

        database_name = db.Integration.query.get(bot_record.database_id).name

        self.chat_handler = self.session.integration_controller.get_data_handler(database_name)
        if not isinstance(self.chat_handler, APIChatHandler):
            raise Exception(f"Can't use chat database: {database_name}")

        chat_params = self.chat_handler.get_chat_config()
        polling = chat_params['polling']['type']

        memory = chat_params['memory']['type'] if 'memory' in chat_params else None
        memory_cls = None
        if memory:
            memory_cls = DBMemory if memory == 'db' else HandlerMemory

        if polling == 'message_count':
            chat_params = chat_params['tables'] if 'tables' in chat_params else [chat_params]
            self.chat_pooling = MessageCountPolling(self, chat_params)
            # The default type for message count polling is HandlerMemory if not specified.
            self.memory = HandlerMemory(self, chat_params) if memory_cls is None else memory_cls(self, chat_params)

        elif polling == 'realtime':
            chat_params = chat_params['tables'] if 'tables' in chat_params else [chat_params]
            self.chat_pooling = RealtimePolling(self, chat_params)
            # The default type for real-time polling is DBMemory if not specified.
            self.memory = DBMemory(self, chat_params) if memory_cls is None else memory_cls(self, chat_params)

        elif polling == 'webhook':
            self.chat_pooling = WebhookPolling(self, chat_params)
            self.memory = DBMemory(self, chat_params)

        else:
            raise Exception(f"Not supported polling: {polling}")

        self.bot_params['bot_username'] = self.chat_handler.get_my_user_name()

    def run(self, stop_event):

        # TODO check deleted, raise errors
        # TODO checks on delete predictor / project/ integration

        self.chat_pooling.run(stop_event)

    def on_message(self, message: ChatBotMessage, chat_id=None, chat_memory=None, table_name=None):
        if not chat_id and not chat_memory:
            raise Exception('chat_id or chat_memory should be provided')

        try:
            self._on_holding_message(chat_id, chat_memory, table_name)
            self._on_message(message, chat_id, chat_memory, table_name)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            error = traceback.format_exc()
            logger.error(error)
            self.set_error(str(error))

    def _on_holding_message(self, chat_id: str = None, chat_memory: BaseMemory = None, table_name: str = None):
        """
        Send a message to hold the user's attention while the bot is processing the request.
        This message will not be saved in the chat memory.

        Args:
            chat_id (str): The ID of the chat.
            chat_memory (BaseMemory): The memory of the chat.
            table_name (str): The name of the table.
        """
        chat_id = chat_id if chat_id else chat_memory.chat_id

        response_message = ChatBotMessage(
            ChatBotMessage.Type.DIRECT,
            HOLDING_MESSAGE,
            # In Slack direct messages are treated as channels themselves.
            user=self.bot_params['bot_username'],
            destination=chat_id,
            sent_at=dt.datetime.now()
        )

        # send to chat adapter
        self.chat_pooling.send_message(response_message, table_name=table_name)
        logger.debug(f'>>chatbot {chat_id} out (holding message): {response_message.text}')

    def _on_message(self, message: ChatBotMessage, chat_id, chat_memory, table_name=None):
        # add question to history
        # TODO move it to realtime pooling
        chat_memory = chat_memory if chat_memory else self.memory.get_chat(chat_id, table_name=table_name)
        chat_memory.add_to_history(message)

        logger.debug(f'>>chatbot {chat_memory.chat_id} in: {message.text}')

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
        self.chat_pooling.send_message(response_message, table_name=table_name)
        logger.debug(f'>>chatbot {chat_id} out: {response_message.text}')

        # send to history
        chat_memory.add_to_history(response_message)

    def on_webhook(self, request: dict) -> None:
        """
        Handle incoming webhook requests.
        Passes the request to the chat handler along with the callback method.

        Args:
            request (dict): The incoming webhook request.
        """
        self.chat_handler.on_webhook(request, self.on_message)

    def get_memory(self) -> BaseMemory:
        """
        Get the memory of the chatbot task.

        Returns:
            BaseMemory: The memory of the chatbot task.
        """
        return self.memory

    def set_memory(self, memory: BaseMemory) -> None:
        """
        Set the memory of the chatbot task.

        Args:
            memory (BaseMemory): The memory to set for the chatbot task.
        """
        self.memory = memory
