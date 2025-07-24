from typing import Dict, Text, Callable, Union

from botbuilder.schema import Activity, ActivityTypes
from botbuilder.schema import ChannelAccount
from botframework.connector import ConnectorClient
from botframework.connector.auth import MicrosoftAppCredentials
import msal
from requests.exceptions import RequestException

from mindsdb.integrations.handlers.ms_teams_handler.ms_graph_api_teams_client import (
    MSGraphAPIBaseClient,
    MSGraphAPITeamsApplicationPermissionsClient,
    MSGraphAPITeamsDelegatedPermissionsClient
)
from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_tables import (
    ChannelsTable, ChannelMessagesTable, ChatsTable, ChatMessagesTable, TeamsTable
)
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.api_handler import APIChatHandler
from mindsdb.integrations.utilities.handlers.auth_utilities.microsoft import (
    MSGraphAPIApplicationPermissionsManager,
    MSGraphAPIDelegatedPermissionsManager
)
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException
from mindsdb.interfaces.chatbot.types import ChatBotMessage
from mindsdb.utilities import log


logger = log.getLogger(__name__)


def chatbot_only(func):
    def wrapper(self, *args, **kwargs):
        if self.connection_data.get('opertion_mode', 'datasource') != 'chatbot':
            raise ValueError("This connection can only be used as a data source. Please use a chatbot connection by setting the 'mode' parameter to 'chat'.")
        return func(self, *args, **kwargs)
    return wrapper


class MSTeamsHandler(APIChatHandler):
    """
    This handler handles the connection and execution of SQL statements on Microsoft Teams via the Microsoft Graph API.
    It is also responsible for handling the chatbot functionality.
    """

    name = 'teams'

    def __init__(self, name: str, **kwargs):
        """
        Initializes the handler.

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

    def connect(self) -> Union[MicrosoftAppCredentials, MSGraphAPIBaseClient]:
        """
        Establishes a connection to the Microsoft Teams registered app or the Microsoft Graph API.

        Returns:
            Union[MicrosoftAppCredentials, MSGraphAPITeamsDelegatedPermissionsClient]: A connection object to the Microsoft Teams registered app or the Microsoft Graph API.
        """
        if self.is_connected:
            return self.connection

        # The default operation mode is 'datasource'. This is used for data source connections.
        operation_mode = self.connection_data.get('operation_mode', 'datasource')
        if operation_mode == 'datasource':
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

            # The default permissions mode is 'delegated'. This requires the user to sign in.
            permission_mode = self.connection_data.get('permission_mode', 'delegated')
            if permission_mode == 'delegated':
                permissions_manager = MSGraphAPIDelegatedPermissionsManager(
                    client_id=self.connection_data['client_id'],
                    client_secret=self.connection_data['client_secret'],
                    tenant_id=self.connection_data['tenant_id'],
                    cache=cache,
                    code=self.connection_data.get('code')
                )

            elif permission_mode == 'application':
                permissions_manager = MSGraphAPIApplicationPermissionsManager(
                    client_id=self.connection_data['client_id'],
                    client_secret=self.connection_data['client_secret'],
                    tenant_id=self.connection_data['tenant_id'],
                    cache=cache
                )

            else:
                raise ValueError("The supported permission modes are 'delegated' and 'application'.")

            access_token = permissions_manager.get_access_token()

            # Save the cache back to file if it has changed.
            if cache.has_state_changed:
                self.handler_storage.file_set(cache_file, cache.serialize().encode('utf-8'))

            if permission_mode == 'delegated':
                self.connection = MSGraphAPITeamsDelegatedPermissionsClient(access_token)

            else:
                self.connection = MSGraphAPITeamsApplicationPermissionsClient(access_token)

            self._register_table('channels', ChannelsTable(self))
            self._register_table('channel_messages', ChannelMessagesTable(self))
            self._register_table('chats', ChatsTable(self))
            self._register_table('chat_messages', ChatMessagesTable(self))
            self._register_table('teams', TeamsTable(self))

        elif operation_mode == 'chatbot':
            self.connection = MicrosoftAppCredentials(
                self.connection_data['app_id'],
                self.connection_data['app_password']
            )

        else:
            raise ValueError("The supported operation modes are 'datasource' and 'chatbot'.")

        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to Microsoft Teams.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            # A connection check against the Microsoft Graph API is run if the connection is in 'datasource' mode.
            if self.connection_data.get('operation_mode', 'datasource') == 'datasource' and connection.check_connection():
                response.success = True
                response.copy_storage = True
            else:
                raise RequestException("Connection check failed!")
        except (ValueError, RequestException) as known_error:
            logger.error(f'Connection check to Microsoft Teams failed, {known_error}!')
            response.error_message = str(known_error)
        except AuthException as error:
            response.error_message = str(error)
            response.redirect_url = error.auth_url
            return response
        except Exception as unknown_error:
            logger.error(f'Connection check to Microsoft Teams failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        self.is_connected = response.success

        return response

    @chatbot_only
    def get_chat_config(self) -> Dict:
        """
        Gets the configuration for the chatbot.
        This method is required for the implementation of the chatbot.

        Returns:
            Dict: The configuration for the chatbot.
        """
        params = {
            'polling': {
                'type': 'webhook'
            }
        }

        return params

    @chatbot_only
    def get_my_user_name(self) -> Text:
        """
        Gets the name of the signed in user.
        This method is required for the implementation of the chatbot.

        Returns:
            Text: The name of the signed in user.
        """
        return None

    @chatbot_only
    def on_webhook(self, request: Dict, callback: Callable) -> None:
        """
        Handles a webhook request.

        Args:
            request (Dict): The request data.
            callback (Callable): The callback function to call.
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

    @chatbot_only
    def respond(self, message: ChatBotMessage) -> None:
        """
        Sends a response to the chatbot.

        Args:
            message (ChatBotMessage): The message to send

        Raises:
            ValueError: If the chatbot message is not of type DIRECT.

        Returns:
            None
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
