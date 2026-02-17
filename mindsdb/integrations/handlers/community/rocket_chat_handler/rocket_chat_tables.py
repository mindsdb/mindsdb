import pandas as pd

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import conditions_to_filter, project_dataframe, sort_dataframe
from mindsdb_sql_parser import ast


def message_to_dataframe_row(message: dict):
    message['id'] = message['_id']
    message['room_id'] = message['rid']
    message['text'] = message['msg']
    message['sent_at'] = message['ts']

    if 'u' in message:
        if 'username' in message['u']:
            message['username'] = message['u']['username']
        if 'name' in message['u']:
            message['name'] = message['u']['name']
    if 'bot' in message and 'i' in message['bot']:
        message['bot_id'] = message['bot']['i']
    return message


class ChannelsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        message_data = self.handler.call_api('channels_list')
        df = pd.DataFrame(message_data['channels'])
        df = project_dataframe(df, query.targets, self.get_columns())

        return df

    def get_columns(self):
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            '_id',
            'name',
            'usersCount',
            'msgs',
        ]


class ChannelMessagesTable(APITable):
    """Manages SELECT and INSERT operations for Rocket Chat messages."""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects message data from the Rocket Chat API and returns it as a pandas DataFrame.

        Returns dataframe representing the Rocket Chat API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        filters = conditions_to_filter(query.where)

        if 'room_id' not in filters:
            raise NotImplementedError()

        params = {}

        if query.limit:
            params['count'] = query.limit

        # See Channel Messages endpoint:
        # https://developer.rocket.chat/reference/api/rest-api/endpoints/core-endpoints/channels-endpoints/messages
        message_data = self.handler.call_api('channels_history', filters['room_id'], **params)

        # Only return the columns we need to.
        message_rows = [message_to_dataframe_row(m) for m in message_data['messages']]
        df = pd.DataFrame(message_rows)

        df = sort_dataframe(df, query.order_by)
        df = project_dataframe(df, query.targets, self.get_columns())

        return df

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

            self.handler.call_api('chat_post_message', **insert_params)

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


class DirectsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        message_data = self.handler.call_api('im_list')
        df = pd.DataFrame(message_data['ims'])
        df = project_dataframe(df, query.targets, self.get_columns())

        return df

    def insert(self, query: ast.Insert):
        column_names = [col.name for col in query.columns]
        for insert_row in query.values:
            insert_params = dict(zip(column_names, insert_row))

            self.handler.call_api('im_create', **insert_params)

    def get_columns(self):
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            '_id',
            'usernames',
            'usersCount',
            'msgs',
        ]


class DirectMessagesTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:

        filters = conditions_to_filter(query.where)

        if 'room_id' not in filters:
            raise NotImplementedError()

        params = {}

        if query.limit:
            params['count'] = query.limit

        message_data = self.handler.call_api('im_history', filters['room_id'], **params)

        message_rows = [message_to_dataframe_row(m) for m in message_data['messages']]
        df = pd.DataFrame(message_rows)
        df = sort_dataframe(df, query.order_by)
        df = project_dataframe(df, query.targets, self.get_columns())

        return df

    def insert(self, query: ast.Insert):

        column_names = [col.name for col in query.columns]
        for insert_row in query.values:
            insert_params = dict(zip(column_names, insert_row))

            if 'username' in insert_params:
                # resolve username
                resp = self.handler.call_api('users_info', insert_params['username'])
                if 'user' in resp:
                    insert_params['room_id'] = resp['user']['_id']
                    del insert_params['username']
                else:
                    raise ValueError(f'User not found: {insert_params["username"]}')

            self.handler.call_api('chat_post_message', **insert_params)

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


class UsersTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        message_data = self.handler.call_api('users_list')
        df = pd.DataFrame(message_data['users'])
        df = project_dataframe(df, query.targets, self.get_columns())

        return df

    def get_columns(self):
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            '_id',
            'username',
            'name',
            'status',
            'active',
            'type',
        ]
