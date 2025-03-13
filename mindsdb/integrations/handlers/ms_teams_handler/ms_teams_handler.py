from typing import Text, Dict, Callable

from botbuilder.schema import Activity, ActivityTypes
from botbuilder.schema import ChannelAccount
from botframework.connector import ConnectorClient
from botframework.connector.auth import MicrosoftAppCredentials
from mindsdb_sql_parser import parse_sql
import msal

from mindsdb.integrations.handlers.ms_teams_handler.ms_graph_api_teams_client import MSGraphAPITeamsClient
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.api_handler import APIChatHandler
from mindsdb.integrations.utilities.handlers.auth_utilities import MSGraphAPIDelegatedPermissionsManager
from mindsdb.interfaces.chatbot.types import ChatBotMessage
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSTeamsHandler(APIChatHandler):
    """
    The Microsoft Teams handler implementation.
    """

    name = 'teams'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.handler_storage = kwargs['handler_storage']
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        self.service_url = None
        self.channel_id = None
        self.bot_id = None
        self.conversation_id = None

    def connect(self) -> MicrosoftAppCredentials:
        """
        Set up the connection required by the handler.

        Returns
        -------
        MicrosoftAppCredentials
            Client object for interacting with the Microsoft Teams app.
        """
        if self.is_connected:
            return self.connection
    
        if self.connection_data.get("is_chatbot", True):
            self.connection = MicrosoftAppCredentials(
                app_id=self.connection_data["client_id"],
                password=self.connection_data["client_secret"]
            )
        else:
            # Initialize the token cache.
            cache = msal.SerializableTokenCache()

            # Load the cache from file if it exists.
            cache_file = 'cache.bin'
            try:
                cache_content = self.handler_storage.file_get(cache_file)
            except FileNotFoundError:
                cache_content = None

            if cache_content:
                cache.deserialize(cache_content)

            # Initialize the Microsoft Authentication Library (MSAL) app.
            permissions_manager = MSGraphAPIDelegatedPermissionsManager(
                client_id=self.connection_data['client_id'],
                client_secret=self.connection_data['client_secret'],
                tenant_id=self.connection_data['tenant_id'],
                cache=cache,
                code=self.connection_data.get('code')
            )

            access_token = permissions_manager.get_access_token()

            # Save the cache back to file if it has changed.
            if cache.has_state_changed:
                self.handler_storage.file_set(cache_file, cache.serialize().encode('utf-8'))

            self.connection = MSGraphAPITeamsClient(access_token)

        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.

        Returns
        -------
        StatusResponse
            Response object with the status of the connection.
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Microsoft Teams: {e}!')
            response.success = False
            response.error_message = str(e)

        self.is_connected = response.success

        return response

    def native_query(self, query: Text) -> StatusResponse:
        """
        Receive and process a raw query.

        Parameters
        ----------
        query: Text
            Query in the native format.

        Returns
        -------
        StatusResponse
            Response object with the result of the query.
        """
        ast = parse_sql(query)

        return self.query(ast)
    
    def get_chat_config(self) -> Dict:
        """
        Get the configuration for the chatbot.
        This method is required for the implementation of the chatbot.

        Returns
        -------
        Dict
            Configuration for the chatbot.
        """
        params = {
            'polling': {
                'type': 'webhook'
            }
        }

        return params
    
    def get_my_user_name(self) -> Text:
        """
        Get the name of the signed in user.
        This method is required for the implementation of the chatbot.

        Returns
        -------
        Text
            Name of the signed in user.
        """
        return None
    
    def on_webhook(self, request: Dict, callback: Callable) -> None:
        """
        Handle a webhook request.

        Parameters
        ----------
        request: Dict
            The incoming webhook request.

        callback: Callable
            Callback function to call after parsing the request.
        """
        self.service_url = request["serviceUrl"]
        self.channel_id = request["channelId"]
        self.bot_id = request["from"]["id"]
        self.conversation_id = request["conversation"]["id"]

        chat_bot_message = ChatBotMessage(
            ChatBotMessage.Type.DIRECT,
            text=request["text"],
            user=request["from"]["id"],
            destination=request["recipient"]["id"]
        )

        callback(
            chat_id=request['conversation']['id'],
            message=chat_bot_message
        )    
            
    def respond(self, message: ChatBotMessage) -> None:
        """
        Send a response to the chatbot.

        Parameters
        ----------
        message: ChatBotMessage
            The message to send.
        """
        credentials = self.connect()

        connector = ConnectorClient(credentials, base_url=self.service_url)
        connector.conversations.send_to_conversation(
            self.conversation_id,
            Activity(
                type=ActivityTypes.message,
                channel_id=self.channel_id,
                recipient=ChannelAccount(
                    id=message.destination
                ),
                from_property=ChannelAccount(
                    id=self.bot_id
                ),
                text=message.text
            )
        )
