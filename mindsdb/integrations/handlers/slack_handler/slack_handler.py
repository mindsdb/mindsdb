from copy import deepcopy
import datetime as dt
import os
import threading
from typing import Any, Callable, Dict, List, Text

import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.socket_mode.request import SocketModeRequest

from mindsdb.integrations.handlers.slack_handler.slack_tables import (
    SlackConversationsTable,
    SlackMessagesTable,
    SlackThreadsTable,
    SlackUsersTable
)
from mindsdb.integrations.libs.api_handler import APIChatHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)
from mindsdb.utilities.config import Config
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SlackHandler(APIChatHandler):
    """
    This handler handles the connection and execution of SQL statements on Slack.
    Additionally, it allows the setup of a real-time connection to the Slack API using the Socket Mode for chat-bots.
    """

    def __init__(self, name: Text, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the SAP HANA database.
            kwargs(Any): Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        # If the parameters are not provided, check the environment variables and the handler configuration.
        handler_config = Config().get('slack_handler', {})

        for key in ['token', 'app_token']:
            if key not in self.connection_data:
                if f'SLACK_{key.upper()}' in os.environ:
                    self.connection_data[key] = os.environ[f'SLACK_{key.upper()}']
                elif key in handler_config:
                    self.connection_data[key] = handler_config[key]

        self.web_connection = None
        self._socket_connection = None
        self.is_connected = False

        self._register_table('conversations', SlackConversationsTable(self))
        self._register_table('messages', SlackMessagesTable(self))
        self._register_table('threads', SlackThreadsTable(self))
        self._register_table('users', SlackUsersTable(self))

    def connect(self) -> WebClient:
        """
        Establishes a connection to the Slack API using the WebClient.

        Returns:
            WebClient: The WebClient object to interact with the Slack API.
        """
        if self.is_connected is True:
            return self.web_connection

        # Check if the mandatory connection parameter (token) is available.
        if 'token' not in self.connection_data:
            raise ValueError('Required parameter (token) must be provided.')

        try:
            self.web_connection = WebClient(token=self.connection_data['token'])
            self.is_connected = True
            return self.web_connection
        except Exception as unknown_error:
            logger.error(f'Unknown error connecting to Slack API: {unknown_error}')
            raise

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Slack API, both for the WebClient and the Socket Mode.

        Raises:
            SlackApiError: If an error occurs while connecting to the Slack API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            web_connection = self.connect()
            # Check the status of the web connection.
            web_connection.auth_test()

            # Check the status of the socket connection if the app_token is provided.
            if 'app_token' in self.connection_data:
                _socket_connection = SocketModeClient(
                    app_token=self.connection_data['app_token'],
                    web_client=web_connection
                )
                _socket_connection.connect()
                _socket_connection.disconnect()

            response.success = True
        except (SlackApiError, ValueError) as known_error:
            logger.error(f'Connection check to the Slack API failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to the Slack API failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: Text = None) -> Response:
        """
        Executes native Slack SDK methods as specified in the query string.

        Args:
            query: The query string containing the method name and parameters.

        Returns:
            Response: A response object containing the result of the query.
        """
        method_name, params = FuncParser().from_string(query)

        df = self._call_slack_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def _call_slack_api(self, method_name: Text = None, params: Dict = None) -> List[Dict]:
        """
        Calls the Slack SDK method with the specified method name and parameters.

        Args:
            method_name (Text): The name of the method to call.
            params (Dict): The parameters to pass to the method.

        Raises:
            SlackApiError: If an error occurs while calling the Slack SDK method

        Returns:
            List[Dict]: The result from running the Slack SDK method.
        """
        web_connection = self.connect()
        method = getattr(web_connection, method_name)

        items = []
        try:
            response = method(**params)
            response_data = deepcopy(response.data)

            # Get only the data items from the response data.
            items.extend(self._extract_data_from_response(response_data))

            # If the response contains a cursor, fetch the next page of results.
            if 'response_metadata' in response and 'next_cursor' in response['response_metadata']:
                while response['response_metadata']['next_cursor']:
                    response = method(cursor=response['response_metadata']['next_cursor'], **params)
                    response_data = deepcopy(response.data)

                    # Get only the data items from the response data.
                    items.extend(self._extract_data_from_response(response_data))
        except SlackApiError as slack_error:
            error = f"Error calling method '{method_name}' with params '{params}': {slack_error.response['error']}"
            logger.error(error)
            raise

        if items:
            df = pd.DataFrame(items)

        return df

    def _extract_data_from_response(self, response_data: Dict) -> List[Dict]:
        """
        Extracts the data items from the response object.

        Args:
            response_data (Dict): The response object containing the data items.

        Raises:
            ValueError: If the response data could not be parsed.

        Returns:
            List[Dict]: The data items extracted from the response object.
        """
        # Remove the metadata from the response.
        for key in ['ok', 'response_metadata', 'cache_ts', 'latest', 'pin_count', 'has_more']:
            if key in response_data:
                response_data.pop(key)

        # If the response contains only one key, return the value of that key as a list.
        if len(response_data) == 1:
            key = list(response_data.keys())[0]
            if isinstance(response_data[key], list):
                return response_data[key]

            else:
                return [response_data[key]]

        # Otherwise, raise an error.
        raise ValueError('Response data could not be parsed.')

    def get_chat_config(self) -> Dict:
        """
        Returns the chat configuration for the Slack handler.

        Returns:
            Dict: The chat configuration.
        """
        return {
            'polling': {
                'type': 'realtime',
            },
            'memory': {
                'type': 'handler',
            },
            'tables': [
                {
                    'chat_table': {
                        'name': 'messages',
                        'chat_id_col': 'channel_id',
                        'username_col': 'user',
                        'text_col': 'text',
                        'time_col': 'created_at',
                    }
                },
                {
                    'chat_table': {
                        'name': 'threads',
                        'chat_id_col': ['channel_id', 'thread_ts'],
                        'username_col': 'user',
                        'text_col': 'text',
                        'time_col': 'thread_ts',
                    }
                }
            ]
        }

    def get_my_user_name(self) -> Text:
        """
        Gets the name of the bot user.

        Returns:
            Text: The name of the bot user.
        """
        web_connection = self.connect()
        user_info = web_connection.auth_test().data
        return user_info['bot_id']

    def subscribe(self, stop_event: threading.Event, callback: Callable, table_name: Text = 'messages',
                  columns: List = None, **kwargs: Any) -> None:
        """
        Subscribes to the Slack API using the Socket Mode for real-time responses to messages.

        Args:
            stop_event (threading.Event): The event to stop the subscription.
            callback (Callable): The callback function to process the messages.
            table_name (Text): The name of the table to subscribe to.
            kwargs: Arbitrary keyword arguments.
        """
        if table_name not in ['messages', 'threads']:
            raise RuntimeError(f'Table {table_name} is not supported for subscription.')

        # Raise an error if columns are provided.
        # Since Slack subscriptions depend on events and not changes to the virtual tables, columns are not supported.
        if columns:
            raise RuntimeError('Columns are not supported for Slack subscriptions.')

        self._socket_connection = SocketModeClient(
            # This app-level token will be used only for establishing a connection.
            app_token=self.connection_data['app_token'],  # xapp-A111-222-xyz
            # The WebClient for performing Web API calls in listeners.
            web_client=WebClient(token=self.connection_data['token']),  # xoxb-111-222-xyz
        )

        def _process_websocket_message(client: SocketModeClient, request: SocketModeRequest) -> None:
            """
            Pre-processes the incoming WebSocket message from the Slack API and calls the callback function to process the message.

            Args:
                client (SocketModeClient): The client object to send the response.
                request (SocketModeRequest): The request object containing the payload.
            """
            # Acknowledge the request.
            response = SocketModeResponse(envelope_id=request.envelope_id)
            client.send_socket_mode_response(response)

            # Ignore requests that are not events.
            if request.type != 'events_api':
                return

            # Ignore duplicate requests.
            if request.retry_attempt is not None and request.retry_attempt > 0:
                return

            payload_event = request.payload['event']
            # Avoid responding to events other than direct messages and app mentions.
            if payload_event['type'] not in ('message', 'app_mention'):
                return

            # Avoid responding to unrelated events like message_changed, message_deleted, etc.
            if 'subtype' in payload_event:
                return

            # Avoid responding to messages from the bot.
            if 'bot_id' in payload_event:
                return

            key = {
                'channel_id': payload_event['channel'],
            }

            row = {
                'text': payload_event['text'],
                'user': payload_event['user'],
                'created_at': dt.datetime.fromtimestamp(float(payload_event['ts'])).strftime('%Y-%m-%d %H:%M:%S')
            }

            # Add thread_ts to the key and row if it is a thread message. This is used to identify threads.
            # This message should be handled via the threads table.
            if 'thread_ts' in payload_event:
                key['thread_ts'] = payload_event['thread_ts']

            callback(row, key)

        self._socket_connection.socket_mode_request_listeners.append(_process_websocket_message)
        self._socket_connection.connect()

        stop_event.wait()

        self._socket_connection.close()
