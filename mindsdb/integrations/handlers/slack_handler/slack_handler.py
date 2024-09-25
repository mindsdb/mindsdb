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

logger = log.getLogger(__name__)

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class SlackConversationsTable(APIResource):

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs
    ) -> pd.DataFrame:
        channels = []
        for condition in conditions:
            value = condition.value
            op = condition.op

            if condition.column == 'id':
                if op not in [FilterOperator.EQUAL, FilterOperator.IN]:
                    raise ValueError(f"Unsupported operator '{op}' for column 'id'")
                
                if op == FilterOperator.EQUAL:
                    try:
                        channels = [self.handler.get_channel(value)]
                        condition.applied = True
                    except ValueError:
                        raise
                    
                if op == FilterOperator.IN:
                    try:
                        channels = self.handler.get_channels(
                            value if isinstance(value, list) else [value]
                        )
                        condition.applied = True
                    except ValueError:
                        raise

        if not channels:
            channels = self.handler.get_limited_channels(limit)

        for channel in channels:
            channel['created_at'] = dt.datetime.fromtimestamp(channel['created'])
            channel['updated_at'] = dt.datetime.fromtimestamp(channel['updated'] / 1000)

        return pd.DataFrame(channels, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return [
            'id',
            'name',
            'is_channel',
            'is_group',
            'is_im',
            'is_mpim',
            'is_private',
            'is_archived',
            'is_general',
            'is_shared',
            'is_ext_shared',
            'is_org_shared',
            'creator',
            'created_at',
            'updated_at',
        ]


class SlackUsersTable(APIResource):

    def list(self, **kwargs) -> pd.DataFrame:
        """
        Retrieves list of users

        The "users.list" call is used in slack api

        :return: pd.DataFrame
        """

        client = self.handler.connect()

        users = client.users_list().data['members']

        return pd.DataFrame(users, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return [
            'id',
            'name',
            'real_name'
        ]


class SlackMessagesTable(APIResource):

    def list(self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs
    ) -> pd.DataFrame:
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

        # Build the filters and parameters for the query
        params = {}

        for condition in conditions:
            value = condition.value
            op = condition.op

            if condition.column == 'channel_id':
                if op != FilterOperator.EQUAL:
                    raise ValueError(f"Unsupported operator '{op}' for column 'channel_id'")

                # Check if the channel exists
                try:
                    channel = self.handler.get_channel(value)
                    params['channel'] = value
                    condition.applied = True
                except SlackApiError as e:
                    raise ValueError(f"Channel '{value}' not found")

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

        if 'channel' not in params:
            raise Exception("To retrieve data from Slack, you need to provide the 'channel_id' parameter.")

        # Retrieve the conversation history
        result = client.conversations_history(**params)

        # convert SlackResponse object to pandas DataFrame
        result = pd.DataFrame(result['messages'], columns=self.get_columns())

        # Remove null rows from the result
        result = result[result['text'].notnull()]

        # Add the selected channel to the dataframe
        result['channel_id'] = params['channel']
        result['channel_name'] = channel['name'] if 'name' in channel else None

        # translate the time stamp into a 'created_at' field
        result['created_at'] = pd.to_datetime(result['ts'].astype(float), unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

        return result

    def get_columns(self) -> List[str]:
        return [
            'channel_id',
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
            if 'channel_id' not in params or 'text' not in params:
                raise Exception("To insert data into Slack, you need to provide the 'channel_id' and 'text' parameters.")

            # post message to Slack channel
            try:
                response = client.chat_postMessage(
                    channel=params['channel_id'],
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

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)

        filters = []
        params = {}

        keys = list(query.update_columns.keys())

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel_id':
                # Check if the channel exists
                try:
                    self.handler.get_channel(arg2)
                    params['channel'] = arg2
                except SlackApiError as e:
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

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)

        filters = []
        params = {}

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel_id':
                # Check if the channel exists
                try:
                    self.handler.get_channel(arg2)
                    params['channel'] = arg2
                except SlackApiError as e:
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
