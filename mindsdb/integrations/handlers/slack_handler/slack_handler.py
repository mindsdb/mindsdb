import os
import datetime as dt
from typing import List
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse

from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.api_handler import APIChatHandler, FuncParser

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from .slack_tables import SlackChannelsTable, SlackThreadsTable

logger = log.getLogger(__name__)



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
        
        channels = SlackChannelsTable(self)
        threads = SlackThreadsTable(self)
        self._register_table('channels', channels)
        self._register_table('threads', threads)

        channel_lists = SlackChannelListsTable(self)
        self._register_table('channel_lists', channel_lists)

        self._socket_mode_client = None

    def get_chat_config(self):
        params = {
            'polling': {
                'type': 'realtime',
                'table_name': 'channels'
            },
            'chat_table': {
                'name': 'channels',
                'chat_id_col': 'channel',
                'username_col': 'user',
                'text_col': 'text',
                'time_col': 'thread_ts',
            }
        }
        return params

    def get_my_user_name(self):
        # TODO
        api = self.connect()
        resp = api.users_profile_get()
        return resp.data['profile']['bot_id']

    def subscribe(self, stop_event, callback, table_name, **kwargs):
        if table_name != 'channels':
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

            payload_event = request.payload['event']
            if payload_event['type'] != 'message':
                return
            if 'subtype' in payload_event:
                # Don't respond to message_changed, message_deleted, etc.
                return
            if payload_event['channel_type'] != 'im':
                # Only support IMs currently.
                return
            if 'bot_id' in payload_event:
                # A bot sent this message.
                return

            key = {
                'channel': payload_event['channel'],
            }
            row = {
                'text': payload_event['text'],
                'user': payload_event['user'],
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
