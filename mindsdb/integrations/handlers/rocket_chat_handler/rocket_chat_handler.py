import pandas as pd
from typing import Dict, List

from mindsdb.integrations.handlers.rocket_chat_handler.rocket_chat_client import RocketChatClient
from mindsdb.integrations.handlers.rocket_chat_handler.rocket_chat_tables import RocketChatMessagesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql import parse_sql


class RocketChatHandler(APIHandler):
    """A class for handling connections and interactions with the Rocket Chat API.

    Attributes:
        username (str): Rocket Chat username to use for authentication.
        password (str): Rocket Chat username to use for authentication.
        auth_token (str): Rocket Chat authorization token to use for all API requests.
        auth_user_id (str): Rocket Chat user ID to associate with all API requests
        domain (str): Path to Rocket Chat domain to use (e.g. https://mindsdb.rocket.chat).
        client (RocketChatClient): The `RocketChatClient` object for interacting with the Rocket Chat API.
        is_connected (bool): Whether or not the API client is connected to the Rocket Chat API.
    """

    def __init__(self, name: str = None, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)
        self.username = None
        self.password = None
        self.auth_token = None
        self.auth_user_id = None
        self.domain = None

        args = kwargs.get('connection_data', {})
        if 'domain' not in args:
            raise ValueError('Must include Rocket Chat "domain" to read and write messages')
        self.domain = args['domain']

        if 'token' in args and 'user_id' in args:
            self.auth_token = args['token']
            self.auth_user_id = args['user_id']
        elif 'username' in args and 'password' in args:
            self.username = args['username']
            self.password = args['password']
        else:
            raise ValueError('Need "token" and "user_id", or "username" and "password" to connect to Rocket Chat')

        self.client = None
        self.is_connected = False

        messages_table = RocketChatMessagesTable(self)
        self._register_table('channel_messages', messages_table)

    def connect(self):
        """Creates a new Rocket Chat API client if needed and sets it as the client to use for requests.

        Returns newly created Rocket Chat API client, or current client if already set.
        """
        if self.is_connected and self.client is not None:
            return self.client

        self.client = RocketChatClient(
            self.domain,
            token=self.auth_token,
            user_id=self.auth_user_id,
            username=self.username,
            password=self.password)

        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        """Checks connection to Rocket Chat API by sending a ping request.

        Returns StatusResponse indicating whether or not the handler is connected.
        """

        response = StatusResponse(False)

        try:
            client = self.connect()
            response.success = client.ping()
        except Exception as e:
            log.logger.error(f'Error connecting to Rocket Chat API: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query, dialect='mindsdb')
        return self.query(ast)

    def _message_to_dataframe_row(self, message: dict) -> List:
        id = message['_id']
        room_id = message['rid']
        message_text = message['msg']
        sent_at = message['ts']
        username = None
        user = None
        bot_id = None
        if 'u' in message:
            username = message['u']['username']
            user = message['u']['name']
        if 'bot' in message:
            bot_id = message['bot']['i']
        return [id, room_id, bot_id, message_text, username, user, sent_at]

    def _get_all_channel_messages(self, params):
        if 'room_id' not in params:
            raise ValueError('Missing "room_id" param to fetch messages for')
        room_id = params['room_id']
        limit = params.get('limit', None)

        client = self.connect()
        all_messages = client.get_all_channel_messages(room_id, limit=limit)
        message_rows = [self._message_to_dataframe_row(m) for m in all_messages]
        return pd.DataFrame(message_rows)

    def _post_message(self, params):
        if 'room_id' not in params:
            raise ValueError('Missing "room_id" param to post message')
        room_id = params['room_id']
        text = params.get('text', None)
        alias = params.get('alias', None)
        emoji = params.get('emoji', None)
        avatar = params.get('avatar', None)

        client = self.connect()
        posted_message = client.post_message(
            room_id,
            text=text,
            alias=alias,
            emoji=emoji,
            avatar=avatar)

        return pd.DataFrame([self._message_to_dataframe_row(posted_message)])

    def call_rocket_chat_api(self, method_name: str = None, params: Dict = None) -> pd.DataFrame:
        """Calls the Rocket Chat API method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            method_name (str): Method name to call
            params (Dict): Params to pass to the API call
        """
        if method_name == 'channels.messages':
            return self._get_all_channel_messages(params)
        elif method_name == 'chat.postMessage':
            return self._post_message(params)
        raise NotImplementedError(f'Method name {method_name} not supported by Rocket Chat API Handler')
