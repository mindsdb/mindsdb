
from mindsdb_sql.parser.ast import Identifier, Select, BinaryOperation, Constant, OrderBy

from mindsdb.interfaces.storage import db


from .types import ChatBotMessage


class BaseMemory:
    '''
    base class to work with chatbot memory
    '''
    MAX_DEPTH = 100

    def __init__(self, chat_task, chat_params):
        # in memory yet
        self._modes = {}
        self._hide_history_before = {}
        self._cache = {}
        self.chat_params = chat_params
        self.chat_task = chat_task

    def get_chat(self, chat_id):
        return ChatMemory(self, chat_id)

    def hide_history(self, chat_id, left_count):
        '''
        set date to start hiding messages
        '''
        history = self.get_chat_history(chat_id)
        if left_count < len(history) - 1:
            left_count = len(history) - 1
        sent_at = history[-left_count].sent_at

        self._hide_history_before[chat_id] = sent_at

    def _apply_hiding(self, chat_id, history):
        '''
        hide messages from history
        '''
        before = self._hide_history_before.get(chat_id)

        if before is None:
            return history

        return [
            msg
            for msg in history
            if msg.sent_at >= before
        ]

    def get_mode(self, chat_id):
        return self._modes.get(chat_id)

    def set_mode(self, chat_id, mode):
        self._modes[chat_id] = mode

    def add_to_history(self, chat_id, chat_message):

        return self._add_to_history(chat_id, chat_message)

    def get_chat_history(self, chat_id, cached=True):
        if cached and chat_id in self._cache:
            history = self._cache[chat_id]
        else:
            history = self._get_chat_history(chat_id)
            self._cache[chat_id] = history

        history = self._apply_hiding(chat_id, history)
        return history

    def _add_to_history(self, chat_id, chat_message):
        raise NotImplementedError

    def _get_chat_history(self, chat_id):
        raise NotImplementedError


class HandlerMemory(BaseMemory):
    '''
    Uses handler's database to store and retrieve messages
    '''

    def _add_to_history(self, chat_id, chat_message):
        # do nothing. sent message will be stored by handler db
        pass

    def _get_chat_history(self, chat_id):
        t_params = self.chat_params['chat_table']

        text_col = t_params['text_col']
        username_col = t_params['username_col']
        time_col = t_params['time_col']

        ast_query = Select(
            targets=[Identifier(text_col),
                     Identifier(username_col),
                     Identifier(time_col)],
            from_table=Identifier(t_params['name']),
            where=BinaryOperation(
                op='=',
                args=[
                    Identifier(t_params['chat_id_col']),
                    Constant(chat_id)
                ]
            ),
            order_by=[OrderBy(Identifier(time_col))],
            limit=Constant(self.MAX_DEPTH),
        )

        resp = self.chat_task.chat_handler.query(ast_query)
        if resp.data_frame is None:
            return

        df = resp.data_frame

        # get last messages
        df = df.iloc[-self.MAX_DEPTH:]

        result = []
        for _, rec in df.iterrows():
            chatbot_message = ChatBotMessage(
                ChatBotMessage.Type.DIRECT,
                rec[text_col],
                user=rec[username_col],
                sent_at=rec[time_col]
            )
            result.append(chatbot_message)

        return result


class DBMemory(BaseMemory):
    '''
    uses mindsdb database to store messages
    '''

    def _add_to_history(self, chat_id, message):

        chat_bot_id = self.chat_task.bot_id
        message = db.ChatBotsHistory(
            chat_bot_id=chat_bot_id,
            type=message.type.name,
            text=message.text,
            user=message.user,
            destination=chat_id,
        )
        db.session.add(message)
        db.session.commit()

    def _get_chat_history(self, chat_id):
        chat_bot_id = self.chat_task.bot_id
        query = db.ChatBotsHistory.query\
            .filter(
                db.ChatBotsHistory.chat_bot_id == chat_bot_id,
                db.ChatBotsHistory.destination == chat_id
            )\
            .order_by(db.ChatBotsHistory.sent_at)\
            .limit(self.MAX_DEPTH)

        result = [
            ChatBotMessage(
                rec.type,
                rec.text,
                rec.user,
                sent_at=rec.sent_at,
            )
            for rec in query
        ]
        return result


class ChatMemory:
    '''
    interface to work with individual chat
    '''
    def __init__(self, memory, chat_id):
        self.memory = memory
        self.chat_id = chat_id

        self.cached = False

    def get_history(self, cached=True):
        result = self.memory.get_chat_history(self.chat_id, cached=cached and self.cached)
        self.cached = True
        return result

    def add_to_history(self, message):
        self.memory.add_to_history(self.chat_id, message)

    def get_mode(self):
        return self.memory.get_mode(self.chat_id)

    def set_mode(self, mode):
        self.memory.set_mode(self.chat_id, mode)

    def hide_history(self, left_count):
        '''
        set date to start hiding messages
        '''
        self.memory.hide_history(self.chat_id, left_count)
