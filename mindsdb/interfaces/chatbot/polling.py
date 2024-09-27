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
                            self.chat_task.on_message(chat_memory, message, table_name=chat_params["chat_table"]["name"])

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

        chat_memory = self.chat_task.memory.get_chat(chat_id)
        self.chat_task.on_message(chat_memory, message)

    def run(self, stop_event):
        t_params = self.params["chat_table"]
        self.chat_task.chat_handler.subscribe(
            stop_event, self._callback, t_params["name"]
        )

    # def send_message(self, message: ChatBotMessage):
    #
    #     self.chat_task.chat_handler.realtime_send(message)
