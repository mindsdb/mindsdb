import pymsteams

from mindsdb.integrations.handlers.ms_teams_handler.ms_teams_tables import MessagesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql import parse_sql

logger = log.getLogger(__name__)


class MSTeamsHandler(APIHandler):
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
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        messages_data = MessagesTable(self)
        self._register_table("messages", messages_data)

    def connect(self):
        """
        Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection

        self.connection = pymsteams.connectorcard(self.connection_data['webhook_url'])

        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Microsoft Teams!')
            response.error_message = str(e)

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw query.
        Parameters
        ----------
        query : str
            query in a native format
        Returns
        -------
        StatusResponse
            Request status
        """
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)