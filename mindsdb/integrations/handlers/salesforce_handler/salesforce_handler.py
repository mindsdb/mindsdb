from typing import Any, Dict, Text

import salesforce_api
from salesforce_api.exceptions import AuthenticationError

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse
)
from mindsdb.integrations.handlers.salesforce_handler.salesforce_tables import ContactsTable
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class SalesforceHandler(APIHandler):
    """
    This handler handles the connection and execution of SQL statements on Salesforce.
    """

    name = 'salesforce'

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
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        self._register_table("contacts", ContactsTable(self))

    def connect(self) -> salesforce_api.client.Client:
        """
        Establishes a connection to the Salesforce API.

        Raises:
            ValueError: If the required connection parameters are not provided.
            AuthenticationError: If an authentication error occurs while connecting to the Salesforce API.

        Returns:
            salesforce_api.client.Client: A connection object to the Salesforce API.
        """
        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['username', 'password', 'client_id', 'client_secret']):
            raise ValueError("Required parameters (username, password, client_id, client_secret) must be provided.")

        try:
            self.connection = salesforce_api.Salesforce(
                username=self.connection_data['username'],
                password=self.connection_data['password'],
                client_id=self.connection_data['client_id'],
                client_secret=self.connection_data['client_secret']
            )
            self.is_connected = True
            return self.connection
        except AuthenticationError as auth_error:
            logger.error(f"Authentication error connecting to Salesforce, {auth_error}!")
            raise
        except Exception as unknown_error:
            logger.error(f"Unknwn error connecting to Salesforce, {unknown_error}!")
            raise

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Salesforce API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except (AuthenticationError, ValueError) as known_error:
            logger.error(f'Connection check to Salesforce failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to Salesforce failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        self.is_connected = response.success

        return response
