from typing import Any, Dict

from mindsdb.integrations.handlers.confluence_handler.confluence_api_client import ConfluenceAPIClient
from mindsdb.integrations.handlers.confluence_handler.confluence_tables import (
    ConfluenceBlogPostsTable,
    ConfluenceDatabasesTable,
    ConfluencePagesTable,
    ConfluenceSpacesTable,
    ConfluenceTasksTable,
    ConfluenceWhiteboardsTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class ConfluenceHandler(APIHandler):
    """
    This handler handles the connection and execution of SQL statements on Confluence.
    """

    name = "confluence"

    def __init__(self, name: str, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (str): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Confluence API.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.thread_safe = True

        self._register_table("spaces", ConfluenceSpacesTable(self))
        self._register_table("pages", ConfluencePagesTable(self))
        self._register_table("blogposts", ConfluenceBlogPostsTable(self))
        self._register_table("whiteboards", ConfluenceWhiteboardsTable(self))
        self._register_table("databases", ConfluenceDatabasesTable(self))
        self._register_table("tasks", ConfluenceTasksTable(self))

    def connect(self) -> ConfluenceAPIClient:
        """
        Establishes a connection to the Confluence API.

        Raises:
            ValueError: If the required connection parameters are not provided.

        Returns:
            atlassian.confluence.Confluence: A connection object to the Confluence API.
        """
        if self.is_connected is True:
            return self.connection

        if not all(key in self.connection_data and self.connection_data.get(key) for key in ['api_base', 'username', 'password']):
            raise ValueError('Required parameters (api_base, username, password) must be provided and should not be empty.')

        self.connection = ConfluenceAPIClient(
            url=self.connection_data.get('api_base'),
            username=self.connection_data.get('username'),
            password=self.connection_data.get('password'),
        )

        self.is_connected = True
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Confluence API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            connection.get_spaces(limit=1)
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Confluence API: {e}!")
            response.error_message = e

        self.is_connected = response.success

        return response
