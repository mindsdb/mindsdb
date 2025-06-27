
from typing import Union

from mindsdb_sql_parser.ast import Identifier, Select, BinaryOperation, Constant, OrderBy

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

    def get_chat(self, chat_id, table_name=None):
        return ChatMemory(self, chat_id, table_name=table_name)

    def hide_history(self, chat_id, left_count, table_name=None):
        '''
        set date to start hiding messages
        '''
        history = self.get_chat_history(chat_id, table_name=table_name)
        if left_count > len(history) - 1:
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

    def add_to_history(self, chat_id, chat_message, table_name=None):

        # If the chat_id is a tuple, convert it to a string when storing the message in the database.
        self._add_to_history(
            chat_id,
            chat_message,
            table_name=table_name
        )
        if chat_id in self._cache:
            del self._cache[chat_id]

    def get_chat_history(self, chat_id, table_name=None, cached=True):
        key = (chat_id, table_name) if table_name else chat_id
        if cached and key in self._cache:
            history = self._cache[key]

        else:
            history = self._get_chat_history(
                chat_id,
                table_name
            )
            self._cache[key] = history

        history = self._apply_hiding(chat_id, history)
        return history

    def _add_to_history(self, chat_id, chat_message, table_name=None):
        raise NotImplementedError

    def _get_chat_history(self, chat_id, table_name=None):
        raise NotImplementedError


class HandlerMemory(BaseMemory):
    '''
    Uses handler's database to store and retrieve messages
    '''

    def _add_to_history(self, chat_id, chat_message, table_name=None):
        # do nothing. sent message will be stored by handler db
        pass

    def _get_chat_history(self, chat_id, table_name):
        t_params = next(
            chat_params['chat_table'] for chat_params in self.chat_params if chat_params['chat_table']['name'] == table_name
        )

        text_col = t_params['text_col']
        username_col = t_params['username_col']
        time_col = t_params['time_col']
        chat_id_cols = t_params['chat_id_col'] if isinstance(t_params['chat_id_col'], list) else [t_params['chat_id_col']]

        chat_id = chat_id if isinstance(chat_id, tuple) else (chat_id,)
        # Add a WHERE clause for each chat_id column.
        where_conditions = [
            BinaryOperation(
                op='=',
                args=[
                    Identifier(chat_id_col),
                    Constant(chat_id[idx])
                ]
            ) for idx, chat_id_col in enumerate(chat_id_cols)
        ]
        # Add a WHERE clause to ignore holding messages from the bot.
        from .chatbot_task import HOLDING_MESSAGE

        where_conditions.append(
            BinaryOperation(
                op='!=',
                args=[
                    Identifier(text_col),
                    Constant(HOLDING_MESSAGE)
                ]
            )
        )

        # Convert the WHERE conditions to a BinaryOperation object.
        where_conditions_binary_operation = None
        for condition in where_conditions:
            if where_conditions_binary_operation is None:
                where_conditions_binary_operation = condition
            else:
                where_conditions_binary_operation = BinaryOperation('and', args=[where_conditions_binary_operation, condition])

        ast_query = Select(
            targets=[Identifier(text_col),
                     Identifier(username_col),
                     Identifier(time_col)],
            from_table=Identifier(t_params['name']),
            where=where_conditions_binary_operation,
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

    def _generate_chat_id_for_db(self, chat_id: Union[str, tuple], table_name: str = None) -> str:
        """
        Generate an ID for the chat to store in the database.
        The ID is a string that includes the components of the chat ID and the table name (if provided) separated by underscores.

        Args:
            chat_id (str | tuple): The ID of the chat.
            table_name (str): The name of the table the chat belongs to.
        """
        if isinstance(chat_id, tuple):
            char_id_str = "_".join(str(val) for val in chat_id)
        else:
            char_id_str = str(chat_id)

        if table_name:
            chat_id_str = f"{table_name}_{char_id_str}"

        return chat_id_str

    def _add_to_history(self, chat_id, message, table_name=None):
        chat_bot_id = self.chat_task.bot_id
        destination = self._generate_chat_id_for_db(chat_id, table_name)

        message = db.ChatBotsHistory(
            chat_bot_id=chat_bot_id,
            type=message.type.name,
            text=message.text,
            user=message.user,
            destination=destination,
        )
        db.session.add(message)
        db.session.commit()

    def _get_chat_history(self, chat_id, table_name=None):
        chat_bot_id = self.chat_task.bot_id
        destination = self._generate_chat_id_for_db(chat_id, table_name)

        query = db.ChatBotsHistory.query\
            .filter(
                db.ChatBotsHistory.chat_bot_id == chat_bot_id,
                db.ChatBotsHistory.destination == destination
            )\
            .order_by(db.ChatBotsHistory.sent_at.desc())\
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
        result.reverse()
        return result


class ChatMemory:
    '''
    interface to work with individual chat
    '''
    def __init__(self, memory, chat_id, table_name=None):
        self.memory = memory
        self.chat_id = chat_id
        self.table_name = table_name

        self.cached = False

    def get_history(self, cached=True):
        result = self.memory.get_chat_history(self.chat_id, self.table_name, cached=cached and self.cached)

        self.cached = True
        return result

    def add_to_history(self, message):
        self.memory.add_to_history(self.chat_id, message, table_name=self.table_name)

    def get_mode(self):
        return self.memory.get_mode(self.chat_id)

    def set_mode(self, mode):
        self.memory.set_mode(self.chat_id, mode)

    def hide_history(self, left_count):
        '''
        set date to start hiding messages
        '''
        self.memory.hide_history(self.chat_id, left_count, table_name=self.table_name)
