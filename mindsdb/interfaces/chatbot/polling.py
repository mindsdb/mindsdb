import time

from mindsdb_sql.parser.ast import Identifier, Select, Insert

from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx

from .types import ChatBotMessage, BotException


class BasePolling:
    def __init__(self, chat_task, chat_params):
        self.params = chat_params
        self.chat_task = chat_task

    def start(self, stop_event):
        raise NotImplementedError

    def send_message(self, message: ChatBotMessage):
        chat_id = message.destination
        text = message.text

        t_params = self.params['chat_table']
        ast_query = Insert(
            table=Identifier(t_params['name']),
            columns=[t_params['chat_id_col'], t_params['text_col']],
            values=[
                [chat_id, text],
            ]
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
                chat_ids = self.check_message_count()
                for chat_id in chat_ids:
                    chat_memory = self.chat_task.memory.get_chat(chat_id)

                    message = self.get_last_message(chat_memory)
                    if message:
                        self.chat_task.on_message(chat_memory, message)

            except Exception as e:
                log.logger.error(e)

            if stop_event.is_set():
                return
            log.logger.debug(f'running {self.chat_task.bot_id}')
            time.sleep(7)

    def get_last_message(self, chat_memory):
        # retrive from history
        history = chat_memory.get_history()
        last_message = history[-1]
        if last_message.user == self.chat_task.bot_params['bot_username']:
            # the last message is from bot
            return
        return last_message

    def check_message_count(self):
        p_params = self.params['polling']

        chat_ids = []

        id_col = p_params['chat_id_col']
        msgs_col = p_params['count_col']
        # get chats status info
        ast_query = Select(
            targets=[
                Identifier(id_col),
                Identifier(msgs_col)],
            from_table=Identifier(p_params['table'])
        )

        resp = self.chat_task.chat_handler.query(query=ast_query)
        if resp.data_frame is None:
            raise BotException('Error to get count of messages')

        chats = {}
        for row in resp.data_frame.to_dict('records'):
            chat_id = row[id_col]
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

        t_params = self.params['chat_table']

        message = ChatBotMessage(
            ChatBotMessage.Type.DIRECT,
            row[t_params['text_col']],
            # In Slack direct messages are treated as channels themselves.
            row[t_params['username_col']],
            row[t_params['chat_id_col']]
        )

        chat_id = row[t_params['chat_id_col']]

        chat_memory = self.chat_task.memory.get_chat(chat_id)
        self.chat_task.on_message(chat_memory, message)

    def run(self, stop_event):
        t_params = self.params['chat_table']
        self.chat_task.chat_handler.subscribe(stop_event, self._callback, t_params['name'])

    # def send_message(self, message: ChatBotMessage):
    #
    #     self.chat_task.chat_handler.realtime_send(message)
