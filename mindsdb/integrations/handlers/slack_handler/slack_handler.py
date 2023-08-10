import os
from datetime import datetime as datetime, timezone
import ast
from typing import List
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse

from mindsdb.interfaces.chatbot.types import ChatBotMessage


from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import ASTNode, Update, Delete
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.integrations.libs.api_handler import APIChatHandler, APITable, FuncParser

from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

class SlackChannelsTable(APITable):
    def __init__(self, handler):
        """
        Checks the connection is active
        """
        super().__init__(handler)
        self.client = WebClient(token=self.handler.connection_args['token'])

    def select(self, query: ast.Select) -> Response:
        """
        Retrieves the data from the channel using SlackAPI

        Args:
            channel_name

        Returns:
            conversation_history
        """
        # override the default function
        def parse_utc_date(date_str):
            date_obj = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
            return date_obj.strftime('%Y-%m-%d %H:%M:%S')

        # Get the channels list and ids
        channels = self.client.conversations_list(types="public_channel,private_channel")['channels']
        channel_ids = {c['name']: c['id'] for c in channels}
        
        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)
        channel_name = conditions[0][2];
        filters = []
        params = {}
        order_by_conditions = {}
        
        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel':
                if arg2 in channel_ids:
                    params['channel'] = channel_ids[arg2]
                else:
                    raise ValueError(f"Channel '{arg2}' not found")

            elif arg1 == 'limit':
                if op == '=': 
                    params['limit'] = int(arg2)
                else:
                    raise NotImplementedError(f'Unknown op: {op}')

            elif arg1 == 'message_created_at':
                date = parse_utc_date(arg2)
                if op == '>':
                    params['start_time'] = date
                elif op == '<':
                    params['end_time'] = date
                else:
                    raise NotImplementedError

            else:
                filters.append([op, arg1, arg2])

        if query.limit:
            params['limit'] = int(query.limit.value)

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[1] == "messages":
                    order_by_conditions["columns"].append("messages")

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        # Retrieve the conversation history
        try:
            result = self.client.conversations_history(channel=params['channel'])
            conversation_history = result["messages"]
        except SlackApiError as e:
            log.logger.error("Error creating conversation: {}".format(e))

        # Get columns for the query and convert SlackResponse object to pandas DataFrame
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # columns to lower case
        columns = [name.lower() for name in columns]

        # convert SlackResponse object to pandas DataFrame
        result = pd.DataFrame(result['messages'], columns=columns)
            
        # add absent columns
        for col in set(columns) & set(result.columns) ^ set(columns):
            result[col] = None

        # filter by columns
        columns = [target.parts[-1].lower() for target in query.targets if isinstance(target, ast.Identifier)]
        result = result[columns]

        # Append the history, timestamp, datetime to the response
        response_history = []
        response_ts = []
        response_datetime = []
        response_datetime_timestamp = []

        # if we have 'message_created_at' attribute, then execute this
        if 'start_time' in params or 'end_time' in params:
            # for every message convert ts to str and ts to DateTime object and message to the response object
            for message in conversation_history:
                timestamp = message['ts']
                float_ts = float(message['ts'])
                datetime_ts = datetime.fromtimestamp(float_ts, tz=timezone.utc)
                datetime_ts = datetime_ts.strftime('%Y-%m-%d %H:%M:%S')
                response_datetime.append(datetime_ts)
                response_ts.append(timestamp)

                # if '>' operator store message in response_datetime_timestamp
                if 'start_time' in params and datetime_ts > params['start_time']:
                    response_datetime_timestamp.append(message['text'])
                # if '<' operator store message in response_datetime_timestamp
                elif 'end_time' in params and datetime_ts < params['end_time']:
                    response_datetime_timestamp.append(message['text'])
                # else store it in response_history
                else:
                    response_history.append(message['text'])

        # else parse message and return as a response object
        else:
            # for every message convert ts to str and ts to DateTime object and message to the response object
            for message in conversation_history:
                timestamp = message['ts']
                float_ts = float(message['ts'])
                datetime_ts = datetime.fromtimestamp(float_ts, tz=timezone.utc)
                datetime_ts = datetime_ts.strftime('%Y-%m-%d %H:%M:%S')
                response_datetime.append(datetime_ts)
                response_ts.append(timestamp)
                response_history.append(message['text'])

        result['channel'] = channel_name
        # convert response_datetime_timestamp to Dataframe object in order to return
        if 'start_time' in params or 'end_time' in params:
            filtered_df = pd.DataFrame(response_datetime_timestamp, columns=result.columns)
            result['messages'] = filtered_df
        else:
            result['messages'] = response_history
        
        result['message_created_at'] = response_datetime
        result['ts'] = response_ts

        # Sort the data based on order_by_conditions
        if len(order_by_conditions.get("columns", [])) > 0:
            result = result.sort_values(
                by=order_by_conditions["columns"],
                ascending=order_by_conditions["ascending"],
            )

        # Limit the result based on the query limit
        if query.limit:
            result = result.head(query.limit.value)
            
        # Alias the target column based on the query
        for target in query.targets:
            if target.alias:
                result.rename(columns={target.parts[-1]: str(target.alias)}, inplace=True)

        # Remove null rows from the result
        result = result[result['messages'].notnull()]
        return result.rename(columns={'messages': 'text'})
        
    def get_columns(self):
        """
        Returns columns from the SlackAPI
        """

        return [
            'ts',
            'text',
            'message_created_at',
            'user',
            'channel',
            'reactions',
            'attachments',
            'thread_ts',
            'reply_count',
            'reply_users_count',
            'latest_reply',
            'subtype',
            'hidden',
        ]

    def insert(self, query):
        """
        Inserts the message in the Slack Channel

        Args:
            channel_name
            message
        """
    
        # get column names and values from the query
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))

            # check if required parameters are provided
            if 'channel' not in params or 'text' not in params:
                raise Exception("To insert data into Slack, you need to provide the 'channel' and 'text' parameters.")

            # post message to Slack channel
            try:
                response = self.client.chat_postMessage(
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

        # Get the channels list and ids
        channels = self.client.conversations_list(types="public_channel,private_channel")['channels']
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

            if keys[0] == 'message':
                params['message'] = str(query.update_columns['message'])
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
            response = self.client.chat_update(
                channel=params['channel'],
                ts=params['ts'],
                text=params['text']
            )
        except SlackApiError as e:
            raise Exception(f"Error updating message in Slack channel '{params['channel']}' with timestamp '{params['ts']}' and message '{params['message']}': {e.response['error']}")

        return response
    
    def delete(self, query: ASTNode):
        """
        Deletes the message in the Slack Channel

        Args:
            channel_name
            ts  [TimeStamp -> Can be found by running select command, the entire result will be printed in the terminal]
        """
        # Get the channels list and ids
        channels = self.client.conversations_list(types="public_channel,private_channel")['channels']
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
            response = self.client.chat_delete(
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
            log.logger.error(response.error_message)

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
            log.logger.error(error)
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
                'created': datetime.fromtimestamp(float(channel['created']))
            }
            new_channels.append(new_channel)
        return new_channels