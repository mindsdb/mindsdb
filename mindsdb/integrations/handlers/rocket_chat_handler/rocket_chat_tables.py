from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast

import pandas as pd


class RocketChatMessagesTable(APITable):
    """Manages SELECT and INSERT operations for Rocket Chat messages."""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects message data from the Rocket Chat API and returns it as a pandas DataFrame.
        
        Returns dataframe representing the Rocket Chat API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        conditions = extract_comparison_conditions(query.where)

        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'room_id':
                if op != '=':
                    raise NotImplementedError
                params['room_id'] = arg2
            if arg1 == 'username':
                if op != '=':
                    raise NotImplementedError
                params['username'] = arg2
        if query.limit:
            params['limit'] = query.limit

        # See IM Messages endpoint:
        # https://developer.rocket.chat/reference/api/rest-api/endpoints/core-endpoints/im-endpoints/messages
        if 'username' in params:
            message_data = self.handler.call_rocket_chat_api(method_name='im.messages', params=params)

        # See Channel Messages endpoint:
        # https://developer.rocket.chat/reference/api/rest-api/endpoints/core-endpoints/channels-endpoints/messages
        else:
            message_data = self.handler.call_rocket_chat_api(method_name='channels.messages', params=params)

        # Only return the columns we need to.
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(message_data) == 0:
            message_data = pd.DataFrame([], columns=columns)
        else:
            # Remove columns not part of select.
            message_data.columns = self.get_columns()
            for col in set(message_data.columns).difference(set(columns)):
                message_data = message_data.drop(col, axis=1)

        return message_data

    def insert(self, query: ast.Insert):
        """Posts a message using the Rocket Chat API.
        
        Args:
            query (ast.Insert): Given SQL INSERT query
        """
        # See Post Message endpoint:
        # https://developer.rocket.chat/reference/api/rest-api/endpoints/core-endpoints/chat-endpoints/postmessage
        column_names = [col.name for col in query.columns]
        for insert_row in query.values:
            insert_params = dict(zip(column_names, insert_row))
            self.handler.call_rocket_chat_api(method_name='chat.postMessage', params=insert_params)

    def get_columns(self):
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'id',
            'room_id',
            'bot_id',
            'text',
            'username',
            'name',
            'sent_at'
        ]
