import overpy

from mindsdb.integrations.handlers.openstreetmap_handler.openstreetmap_tables import (OpenStreetMapNodeTable,
                                                                                      OpenStreetMapWayTable, OpenStreetMapRelationTable)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


class OpenStreetMapHandler(APIHandler):
    """The OpenStreetMap handler implementation."""

    def __init__(self, name: str, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        nodes_data = OpenStreetMapNodeTable(self)
        self._register_table("nodes", nodes_data)

        ways_data = OpenStreetMapWayTable(self)
        self._register_table("ways", ways_data)

        relations_data = OpenStreetMapRelationTable(self)
        self._register_table("relations", relations_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns:
            StatusResponse: connection object
        """
        if self.is_connected is True:
            return self.connection

        api_session = overpy.Overpass()

        self.connection = api_session

        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)

        try:
            api_session = self.connect()
            if api_session is not None:
                response.success = True
        except Exception as e:
            logger.error('Error connecting to OpenStreetMap!')
            response.error_message = str(e)

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> Response:
        """Execute a native query on the handler.

        Args:
            query (str): The query to execute.

        Returns:
            Response: The response from the query.
        """
        ast = parse_sql(query)
        return self.query(ast)
