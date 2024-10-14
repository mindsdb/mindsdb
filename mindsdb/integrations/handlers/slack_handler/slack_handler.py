import os
import json
import datetime as dt
from typing import List
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import ASTNode
from mindsdb.integrations.libs.api_handler import APIChatHandler, APIResource, FuncParser
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator

from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers.slack_handler.slack_tables import (
    SlackMessagesTable,
    SlackConversationsTable,
    SlackUsersTable
)

logger = log.getLogger(__name__)

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class SlackHandler(APIChatHandler):
    """
    A class for handling connections and interactions with Slack API.
    Agrs:
        bot_token(str): The bot token for the Slack app.
    """

    def __init__(self, name=None, **kwargs):
        """
        Initializes the connection by checking all the params are provided by the user.
        """
        super().__init__(name)

        args = kwargs.get('connection_data', {})
        self.handler_storage = kwargs.get('handler_storage')
        self.connection_args = {}
        handler_config = Config().get('slack_handler', {})
        for k in ['token', 'app_token']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'SLACK_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'SLACK_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]
        self.api = None
        self.is_connected = False
        
        channels = SlackMessagesTable(self)
        self._register_table('messages', channels)

        channel_lists = SlackConversationsTable(self)
        self._register_table('conversations', channel_lists)

        users = SlackUsersTable(self)
        self._register_table('users', users)

        self._socket_mode_client = None

    def get_chat_config(self):
        params = {
            'polling': {
                'type': 'realtime',
                'table_name': 'messages'
            },
            'chat_table': {
                'name': 'messages',
                'chat_id_col': 'channel_id',
                'username_col': 'user',
                'text_col': 'text',
                'time_col': 'thread_ts',
            }
        }
        return params

    def get_my_user_name(self):
        api = self.connect()
        user_info = api.auth_test().data
        return user_info['bot_id']

    def subscribe(self, stop_event, callback, table_name, **kwargs):
        if table_name != 'messages':
            raise RuntimeError(f'Table not supported: {table_name}')

        self._socket_mode_client = SocketModeClient(
            # This app-level token will be used only for establishing a connection
            app_token=self.connection_args['app_token'],  # xapp-A111-222-xyz
            # You will be using this WebClient for performing Web API calls in listeners
            web_client=WebClient(token=self.connection_args['token']),  # xoxb-111-222-xyz
        )

        def _process_websocket_message(client: SocketModeClient, request: SocketModeRequest):
            # Acknowledge the request
            response = SocketModeResponse(envelope_id=request.envelope_id)
            client.send_socket_mode_response(response)

            if request.type != 'events_api':
                return

            # Ignore duplicated requests
            if request.retry_attempt is not None and request.retry_attempt > 0:
                return

            payload_event = request.payload['event']

            if payload_event['type'] not in ('message', 'app_mention'):
                # TODO: Refresh the channels cache
                return

            if 'subtype' in payload_event:
                # Avoid responding to message_changed, message_deleted, etc.
                return

            if 'bot_id' in payload_event:
                # Avoid responding to messages from the bot
                return

            key = {
                'channel_id': payload_event['channel'],
            }
            row = {
                'text': payload_event['text'],
                'user': payload_event['user'],
                'channel_id': payload_event['channel'],
                'created_at': dt.datetime.fromtimestamp(float(payload_event['ts'])).strftime('%Y-%m-%d %H:%M:%S')
            }

            callback(row, key)

        self._socket_mode_client.socket_mode_request_listeners.append(_process_websocket_message)
        self._socket_mode_client.connect()

        stop_event.wait()

        self._socket_mode_client.close()

    def create_connection(self):
        """
        Creates a WebClient object to connect to the Slack API token stored in the connection_args attribute.
        """

        client = WebClient(token=self.connection_args['token'])
        return client
    
    def connect(self):
        """
        Authenticate with the Slack API using the token stored in the `token` attribute.
        """
        if self.is_connected is True:
            return self.api

        self.api = self.create_connection()
        return self.api

    def check_connection(self):
        """
        Checks the connection by calling auth_test()
        """
        response = StatusResponse(False)

        try:
            api = self.connect()

            # Call API method to check the connection
            api.auth_test()

            # check app_token
            if 'app_token' in self.connection_args:
                socket_client = SocketModeClient(
                    app_token=self.connection_args['app_token'],
                    web_client=api
                )
                socket_client.connect()
                socket_client.disconnect()
            
            response.success = True
        except SlackApiError as e:
            response.error_message = f'Error connecting to Slack Api: {e.response["error"]}. Check token.'
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        """
        Parses the query with FuncParser and calls call_slack_api and returns the result of the query as a Response object.
        """
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_slack_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_slack_api(self, method_name: str = None, params: dict = None):
        """
        Calls specific method specified.

        Args:
            method_name: to call specific method
            params: parameters to call the method

        Returns:
            List of dictionaries as a result of the method call
        """
        api = self.connect()
        method = getattr(api, method_name)

        try:
            result = method(**params)

        except SlackApiError as e:
            error = f"Error calling method '{method_name}' with params '{params}': {e.response['error']}"
            logger.error(error)
            raise e

        if 'channels' in result:
            result['channels'] = self.convert_channel_data(result['channels'])

        return [result]
    
    def get_channel(self, channel_id: str):
        """
        Get the channel data by channel id.

        Args:
            channel_id: str
                The channel id.

        Returns:
            dict
                The channel data.
        """
        client = self.connect()

        try:
            response = client.conversations_info(channel=channel_id)
        except SlackApiError as e:
            logger.error(f"Error getting channel '{channel_id}': {e.response['error']}")
            raise ValueError(f"Channel '{channel_id}' not found")

        return response['channel']
    
    def get_channels(self, channel_ids: List[str]):
        """
        Get the channel data by channel ids.

        Args:
            channel_ids: List[str]
                The channel ids.

        Returns:
            List[dict]
                The channel data.
        """
        # TODO: Handle rate limiting
        channels = []
        for channel_id in channel_ids:
            try:
                channel = self.get_channel(channel_id)
                channels.append(channel)
            except SlackApiError:
                logger.error(f"Channel '{channel_id}' not found")
                raise ValueError(f"Channel '{channel_id}' not found")
                
        return channels

    def get_limited_channels(self, limit: int = None):
        """
        Get the list of channels with a limit.
        If the provided limit is greater than 1000, provide no limit to the API call and paginate the results until the limit is reached.

        Args:
            limit: int
                The limit of the channels to return.

        Returns:
            List[dict]
                The list of channels.
        """
        client = self.connect()

        try:
            if limit and limit > 1000:
                response = client.conversations_list()
                channels = response['channels']

                while response['response_metadata']['next_cursor']:
                    response = client.conversations_list(cursor=response['response_metadata']['next_cursor'])
                    channels.extend(response['channels'])
                    if len(channels) >= limit:
                        break

                channels = channels[:limit]
            else:
                response = client.conversations_list(limit=limit if limit else 1000)
                channels = response['channels']
        except SlackApiError as e:
            logger.error(f"Error getting channels: {e.response['error']}")
            raise ValueError(f"Error getting channels: {e.response['error']}")

        return channels

    def convert_channel_data(self, channels: List[dict]):
        """
        Convert the list of channel dictionaries to a format that can be easily used in the data pipeline.

        Args:
            channels: A list of channel dictionaries.

        Returns:
            A list of channel dictionaries with modified keys and values.
        """

        new_channels = []
        for channel in channels:
            new_channel = {
                'id': channel['id'],
                'name': channel['name'],
                'created': dt.datetime.fromtimestamp(float(channel['created']))
            }
            new_channels.append(new_channel)
        return new_channels
