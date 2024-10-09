from typing import Any, Dict, Text

import salesforce_api

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse
)
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
            connection_data (Dict): The connection data required to connect to the Salesforce account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self) -> salesforce_api.client.Client:
        """
        Establishes a connection to the Salesforce account.

        Raises:
            

        Returns:
            salesforce_api.client.Client: A connection object to the Salesforce account.
        """
        pass
        
    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Salesforce account.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        pass