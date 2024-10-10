import secrets
import threading
import time

from mindsdb_sql.parser.ast import Identifier, Select, Insert

from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx

from .types import ChatBotMessage, BotException

logger = log.getLogger(__name__)


class BasePolling:
    def __init__(self, chat_task, chat_params):
        self.params = chat_params
        self.chat_task = chat_task

    def start(self, stop_event):
        raise NotImplementedError

    def send_message(self, message: ChatBotMessage, table_name=None):
        chat_id = message.destination if isinstance(message.destination, tuple) else (message.destination,)
        text = message.text

        t_params = self.params["chat_table"] if table_name is None else next(
            (t["chat_table"] for t in self.params if t["chat_table"]["name"] == table_name)
        )
        chat_id_cols = t_params["chat_id_col"] if isinstance(t_params["chat_id_col"], list) else [t_params["chat_id_col"]]

        ast_query = Insert(
            table=Identifier(t_params["name"]),
            columns=[*chat_id_cols, t_params["text_col"]],
            values=[
                [*chat_id, text],
            ],
        )

        self.chat_task.chat_handler.query(ast_query)


class MessageCountPolling(BasePolling):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._to_stop = False
        self.chats_prev = None

    def run(self, stop_event):
        while True:
            try:
                for chat_params in self.params:
                    chat_ids = self.check_message_count(chat_params)
                    logger.debug(f"number of chat ids found: {len(chat_ids)}")

                    for chat_id in chat_ids:
                        try:
                            chat_memory = self.chat_task.memory.get_chat(
                                chat_id,
                                table_name=chat_params["chat_table"]["name"],
                            )
                        except Exception as e:
                            logger.error(f"Problem retrieving chat memory: {e}")

                        try:
                            message = self.get_last_message(chat_memory)
                        except Exception as e:
                            logger.error(f"Problem getting last message: {e}")
                            message = None

                        if message:
                            self.chat_task.on_message(message, chat_memory=chat_memory, table_name=chat_params["chat_table"]["name"])

            except Exception as e:
                logger.error(e)

            if stop_event.is_set():
                return
            logger.debug(f"running {self.chat_task.bot_id}")
            time.sleep(7)

    def get_last_message(self, chat_memory):
        # retrive from history
        try:
            history = chat_memory.get_history()
        except Exception as e:
            logger.error(f"Problem retrieving history: {e}")
            history = []
        last_message = history[-1]
        if last_message.user == self.chat_task.bot_params["bot_username"]:
            # the last message is from bot
            return
        return last_message

    def check_message_count(self, chat_params):
        p_params = chat_params["polling"]

        chat_ids = []

        id_cols = p_params["chat_id_col"] if isinstance(p_params["chat_id_col"], list) else [p_params["chat_id_col"]]
        msgs_col = p_params["count_col"]
        # get chats status info
        ast_query = Select(
            targets=[*[Identifier(id_col) for id_col in id_cols], Identifier(msgs_col)],
            from_table=Identifier(p_params["table"]),
        )

        resp = self.chat_task.chat_handler.query(query=ast_query)
        if resp.data_frame is None:
            raise BotException("Error to get count of messages")

        chats = {}
        for row in resp.data_frame.to_dict("records"):
            chat_id = tuple(row[id_col] for id_col in id_cols)
            msgs = row[msgs_col]

            chats[chat_id] = msgs

        if self.chats_prev is None:
            # first run
            self.chats_prev = chats
        else:
            # compare
            # for new keys
            for chat_id, count_msgs in chats.items():
                if self.chats_prev.get(chat_id) != count_msgs:
                    chat_ids.append(chat_id)

            self.chats_prev = chats
        return chat_ids

    def stop(self):
        self._to_stop = True


class RealtimePolling(BasePolling):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # call back can be without context
        self._ctx_dump = ctx.dump()

    def _callback(self, row, key):
        ctx.load(self._ctx_dump)

        row.update(key)

        t_params = self.params["chat_table"]

        message = ChatBotMessage(
            ChatBotMessage.Type.DIRECT,
            row[t_params["text_col"]],
            # In Slack direct messages are treated as channels themselves.
            row[t_params["username_col"]],
            row[t_params["chat_id_col"]],
        )

        chat_id = row[t_params["chat_id_col"]]

        self.chat_task.on_message(message, chat_id=chat_id)

    def run(self, stop_event):
        t_params = self.params["chat_table"]
        self.chat_task.chat_handler.subscribe(
            stop_event, self._callback, t_params["name"]
        )

    # def send_message(self, message: ChatBotMessage):
    #
    #     self.chat_task.chat_handler.realtime_send(message)


class WebhookPolling(BasePolling):
    """
    Polling class for handling webhooks.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, stop_event: threading.Event) -> None:
        """
        Run the webhook polling.
        Check if a webhook token is set for the chatbot. If not, generate a new one.
        Then, do nothing, as the webhook is handled by a task instantiated for each request.

        Args:
            stop_event (threading.Event): Event to stop the polling.
        """
        # If a webhook token is not set for the chatbot, generate a new one.
        from mindsdb.interfaces.chatbot.chatbot_controller import ChatBotController

        chat_bot_controller = ChatBotController()
        chat_bot = chat_bot_controller.get_chatbot_by_id(self.chat_task.object_id)

        if not chat_bot["webhook_token"]:
            chat_bot_controller.update_chatbot(
                chatbot_name=chat_bot["name"],
                project_name=chat_bot["project"],
                webhook_token=secrets.token_urlsafe(32),
            )

        # Do nothing, as the webhook is handled by a task instantiated for each request.
        stop_event.wait()

    def send_message(self, message: ChatBotMessage, table_name: str = None) -> None:
        """
        Send a message (response) to the chatbot.
        Pass the message to the chatbot handler to respond.

        Args:
            message (ChatBotMessage): The message to send.
            table_name (str): The name of the table to send the message to. Defaults to None.
        """
        self.chat_task.chat_handler.respond(message)
