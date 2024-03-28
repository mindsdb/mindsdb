import os
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

logger = log.getLogger(__name__)

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class SlackChannelListsTable(APIResource):

    def list(self, **kwargs) -> pd.DataFrame:

        client = self.handler.connect()

        channels = client.conversations_list(types="public_channel,private_channel")['channels']

        for channel in channels:
            channel['created_at'] = dt.datetime.fromtimestamp(channel['created'])
            channel['updated_at'] = dt.datetime.fromtimestamp(channel['updated'] / 1000)

        return pd.DataFrame(channels, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return [
            'id',
            'name',
            'created_at',
            'updated_at'
        ]


class SlackChannelsTable(APIResource):

    def list(self,
             conditions: List[FilterCondition] = None,
             limit: int = None,
             **kwargs) -> pd.DataFrame:
        """
        Retrieves the data from the channel using SlackAPI

        Returns:
            conversation_history, pd.DataFrame
        """

        client = self.handler.connect()

        # override the default function
        def parse_utc_date(date_str):
            date_obj = dt.datetime.fromisoformat(date_str).replace(tzinfo=dt.timezone.utc)
            return date_obj

        # Get the channels list and ids
        channels = client.conversations_list(types="public_channel,private_channel")['channels']
        channel_ids = {c['name']: c['id'] for c in channels}
        
        # Extract comparison conditions from the query
        channel_name = None

        # Build the filters and parameters for the query
        params = {}

        for condition in conditions:
            value = condition.value
            op = condition.op

            if condition.column == 'channel':
                if value in channel_ids:
                    params['channel'] = channel_ids[value]
                    channel_name = value
                    condition.applied = True
                else:
                    raise ValueError(f"Channel '{value}' not found")

            # Is this used?
            # elif condition.column == 'limit':
            #     if op == FilterOperator.EQUAL:
            #         params['limit'] = int(value)
            #         condition.applied = True
            #     else:
            #         raise NotImplementedError(f'Unknown op: {op}')

            elif condition.column == 'created_at' and value is not None:
                date = parse_utc_date(value)
                if op == FilterOperator.GREATER_THAN:
                    params['oldest'] = date.timestamp() + 1
                elif op == FilterOperator.GREATER_THAN_OR_EQUAL:
                    params['oldest'] = date.timestamp()
                elif op == FilterOperator.LESS_THAN_OR_EQUAL:
                    params['latest'] = date.timestamp()
                else:
                    continue
                condition.applied = True

        if limit:
            params['limit'] = limit

        # Retrieve the conversation history
        result = client.conversations_history(**params)

        # convert SlackResponse object to pandas DataFrame
        result = pd.DataFrame(result['messages'], columns=self.get_columns())

        # Remove null rows from the result
        result = result[result['text'].notnull()]

        # Add the selected channel to the dataframe
        result['channel'] = channel_name

        # translate the time stamp into a 'created_at' field
        result['created_at'] = pd.to_datetime(result['ts'].astype(float), unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

        return result

    def get_columns(self) -> List[str]:
        return [
            'channel',
            'client_msg_id',
            'type',
            'subtype',
            'ts',
            'created_at',
            'user',
            'text',
            'attachments',
            'files',
            'reactions',
            'thread_ts',
            'reply_count',
            'reply_users_count',
            'latest_reply',
            'reply_users'
        ]

    def insert(self, query):
        """
        Inserts the message in the Slack Channel

        Args:
            channel_name
            message
        """
        client = self.handler.connect()
    
        # get column names and values from the query
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))

            # check if required parameters are provided
            if 'channel' not in params or 'text' not in params:
                raise Exception("To insert data into Slack, you need to provide the 'channel' and 'text' parameters.")

            # post message to Slack channel
            try:
                response = client.chat_postMessage(
                    channel=params['channel'],
                    text=params['text']
                )
            except SlackApiError as e:
                raise Exception(f"Error posting message to Slack channel '{params['channel']}': {e.response['error']}")
            
            inserted_id = response['ts']
            params['ts'] = inserted_id

    def update(self, query: ast.Update):
        """
        Updates the message in the Slack Channel

        Args:
            updated message
            channel_name
            ts  [TimeStamp -> Can be found by running select command, the entire result will be printed in the terminal]
        """
        client = self.handler.connect()

        # Get the channels list and ids
        channels = client.conversations_list(types="public_channel,private_channel")['channels']
        channel_ids = {c['name']: c['id'] for c in channels}

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)

        filters = []
        params = {}

        keys = list(query.update_columns.keys())

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel':
                if arg2 in channel_ids:
                    params['channel'] = channel_ids[arg2]
                else:
                    raise ValueError(f"Channel '{arg2}' not found")

            if keys[0] == 'text':
                params['text'] =  str(query.update_columns['text'])
            else:
                raise ValueError(f"Message '{arg2}' not found")

            if arg1 == 'ts':
                if op == '=':
                    params['ts'] = float(arg2)
                else:
                    raise NotImplementedError(f'Unknown op: {op}')
            else:
                filters.append([op, arg1, arg2])

        # check if required parameters are provided
        if 'channel' not in params or 'ts' not in params or 'text' not in params:
            raise Exception("To update a message in Slack, you need to provide the 'channel', 'ts', and 'text' parameters.")

        # update message in Slack channel
        try:
            response = client.chat_update(
                channel=params['channel'],
                ts=str(params['ts']),
                text=params['text'].strip()
            )
        except SlackApiError as e:
            raise Exception(f"Error updating message in Slack channel '{params['channel']}' with timestamp '{params['ts']}' and message '{params['text']}': {e.response['error']}")

    def delete(self, query: ASTNode):
        """
        Deletes the message in the Slack Channel

        Args:
            channel_name
            ts  [TimeStamp -> Can be found by running select command, the entire result will be printed in the terminal]
        """
        client = self.handler.connect()

        # Get the channels list and ids
        channels = client.conversations_list(types="public_channel,private_channel")['channels']
        channel_ids = {c['name']: c['id'] for c in channels}
        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)

        filters = []
        params = {}

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel':
                if arg2 in channel_ids:
                    params['channel'] = channel_ids[arg2]
                else:
                    raise ValueError(f"Channel '{arg2}' not found")

            if arg1 == 'ts':
                if op == '=':
                    params['ts'] = float(arg2)
                else:
                    raise NotImplementedError(f'Unknown op: {op}')

            else:
                filters.append([op, arg1, arg2])

        # check if required parameters are provided
        if 'channel' not in params or 'ts' not in params:
            raise Exception("To delete a message from Slack, you need to provide the 'channel' and 'ts' parameters.")

        # delete message from Slack channel
        try:
            response = client.chat_delete(
                channel=params['channel'],
                ts=params['ts']
            )
            
        except SlackApiError as e:
            raise Exception(f"Error deleting message from Slack channel '{params['channel']}' with timestamp '{params['ts']}': {e.response['error']}")


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
        self._register_table('channels', channels)

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
