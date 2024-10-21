import datetime as dt
import os
import threading
from typing import Any, Callable, Dict, List, Text

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse

from mindsdb.integrations.libs.api_handler import APIChatHandler, FuncParser
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
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)

        # Check if the required connection parameters are provided either in the connection_data, environment variables, or handler configuration.
        handler_config = Config().get('slack_handler', {})
        for parameter in ['token', 'app_token']:
            if parameter in connection_data:
                self.connection_data[parameter] = connection_data[parameter]

            elif f'SLACK_{parameter.upper()}' in os.environ:
                self.connection_data[parameter] = os.environ[f'SLACK_{parameter.upper()}']

            elif parameter in handler_config:
                self.connection_data[parameter] = handler_config[parameter]

        self.kwargs = kwargs
        
        # Register API tables.
        self._register_table('messages', SlackMessagesTable(self))

        self._register_table('conversations', SlackConversationsTable(self))

        self._register_table('users', SlackUsersTable(self))

        self.web_connection = None
        self._socket_connection = None
        self.is_connected = False

    def connect(self) -> WebClient:
        """
        Establishes a web connection to the Slack API.

        Returns:
            WebClient: A connection object to the Slack API.
        """
        if self.is_connected is True:
            return self.web_connection

        self.web_connection = WebClient(token=self.connection_data['token'])
        return self.web_connection
    
    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection (web and socket) to the Slack API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            # Check the status of the web connection.
            web_connection = self.connect()
            web_connection.auth_test()

            # Check the status of the socket connection if the app token is provided.
            if 'app_token' in self.connection_data:
                socket_connection = SocketModeClient(
                    app_token=self.connection_data['app_token'],
                    web_client=web_connection
                )
                socket_connection.connect()
                socket_connection.disconnect()
            
            response.success = True
        except SlackApiError as slack_error:
            logger.error(f'Connection check to Slack failed, {slack_error}!')
            response.error_message = str(slack_error)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response
    
    def native_query(self, query_string: Text = None) -> Response:
        """
        Executes native Slack SDK methods as specified in the query string.

        Args:
            query_string (Text): The query string containing the method name and parameters.

        Returns:
            Response: A response object containing the result of the query.
        """
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_slack_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )
    
    def call_slack_api(self, method_name: Text = None, params: Dict = None) -> List[Dict]:
        """
        Calls the Slack SDK method with the specified method name and parameters.

        Args:
            method_name (Text): The name of the method to call.
            params (Dict): The parameters to pass to the method.

        Returns:
            List of dictionaries as a result of the method call
        """
        web_connection = self.connect()
        method = getattr(web_connection, method_name)

        try:
            result = method(**params)
        except SlackApiError as e:
            logger.error(f"Error calling method '{method_name}' with params '{params}': {e.response['error']}")
            raise

        if 'channels' in result:
            result['channels'] = self.convert_channel_data(result['channels'])

        return [result]

    def get_chat_config(self) -> Dict:
        """
        Returns the chat configuration for the Slack handler.

        Returns:
            Dict: The chat configuration.
        """
        return {
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

    def get_my_user_name(self) -> Text:
        """
        Get the name of the bot user.

        Returns:
            Text: The name of the bot user.
        """
        web_connection = self.connect()
        user_info = web_connection.auth_test().data
        return user_info['bot_id']

    def subscribe(self, stop_event: threading.Event, callback: Callable, table_name: Text, **kwargs: Any) -> None:
        """
        Subscribes to the Slack API using the Socket Mode for real-time responses to messages.

        Args:
            stop_event (threading.Event): The event to stop the subscription.
            callback (Callable): The callback function to process the messages.
            table_name (Text): The name of the table to subscribe to.
            kwargs: Arbitrary keyword arguments.

        Raises:
            RuntimeError: If the table name is not supported.        
        """
        if table_name != 'messages':
            raise RuntimeError(f'Table not supported: {table_name}')

        self._socket_connection = SocketModeClient(
            app_token=self.connection_data['app_token'],
            web_client=self.connect()
        )

        def _process_websocket_message(client: SocketModeClient, request: SocketModeRequest):
            """
            Pre-processes the incoming WebSocket message from the Slack API and calls the callback function to process the message.

            Args:
                client (SocketModeClient): The client object to send the response.
                request (SocketModeRequest): The request object containing the payload.
            """
            # Acknowledge the request.
            response = SocketModeResponse(envelope_id=request.envelope_id)
            client.send_socket_mode_response(response)

            if request.type != 'events_api':
                return

            # Ignore duplicated requests.
            if request.retry_attempt is not None and request.retry_attempt > 0:
                return

            payload_event = request.payload['event']

            if payload_event['type'] not in ('message', 'app_mention'):
                return

            if 'subtype' in payload_event:
                # Avoid responding to message_changed, message_deleted, etc.
                return

            if 'bot_id' in payload_event:
                # Avoid responding to messages from the bot.
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

        self._socket_connection.socket_mode_request_listeners.append(_process_websocket_message)
        self._socket_connection.connect()

        stop_event.wait()

        self._socket_connection.close()
    
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
