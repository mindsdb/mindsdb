from typing import Any, Dict, Text

import msal

from mindsdb.integrations.handlers.ms_one_drive_handler.ms_graph_api_one_drive_client import MSGraphAPIOneDriveClient
from mindsdb.integrations.handlers.ms_one_drive_handler.ms_one_drive_tables import ListFilesTable
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.utilities.handlers.auth_utilities import MSGraphAPIAuthManager
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSOneDriveHandler(APIHandler):
    """
    This handler handles the connection and execution of SQL statements on Microsoft OneDrive.
    """

    name = 'one_drive'

    def __init__(self, name: Text, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Salesforce API.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.handler_storage = kwargs['handler_storage']
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        # Register Microsoft OneDrive tables.
        self._register_table("files", ListFilesTable(self))

    def connect(self):
        """
        Establishes a connection to Microsoft OneDrive via the Microsoft Graph API.

        Raises:
            ValueError: If the required connection parameters are not provided.
            

        Returns:
            
        """
        if self.is_connected and self.connection.check_connection():
            return self.connection
        
        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['client_id', 'client_secret', 'tenant_id']):
            raise ValueError("Required parameters (client_id, client_secret, tenant_id) must be provided.")

        # Initialize the Microsoft Authentication Library (MSAL) app.
        app = msal.ConfidentialClientApplication(
            self.connection_data["client_id"],
            authority=f"https://login.microsoftonline.com/{self.connection_data['tenant_id']}",
            client_credential=self.connection_data["client_secret"],
        )

        # Get the access token from the app.
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        if 'access_token' in result:
            access_token = result['access_token']

        else:
            raise Exception(result.get("error_description"))

        # Pass the access token to the Microsoft Graph API client for Microsoft OneDrive.
        self.connection = MSGraphAPIOneDriveClient(
            access_token=access_token,
            user_principal_name=self.connection_data['email']
        )

        self.is_connected = True

        return self.connection
    
    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Microsoft Graph API for Microsoft OneDrive.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            connection.check_connection()
            response.success = True
            response.copy_storage = True
        except (ValueError) as known_error:
            logger.error(f'Connection check to Microsoft OneDrive failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to Microsoft OneDrive failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        self.is_connected = response.success

        return response