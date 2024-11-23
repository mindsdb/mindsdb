from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.luma_handler.luma_tables import (
    LumaEventsTable
)
from mindsdb.integrations.handlers.luma_handler.luma import LumaClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)


class LumaHandler(APIHandler):
    """The Luma handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the Luma handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.luma_client = None
        self.is_connected = False

        events_data = LumaEventsTable(self)
        self._register_table("events", events_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        try:
            self.luma_client = LumaClient(self.connection_data.get("api_key"))
            resp.success = True
            self.is_connected = True
        except Exception as ex:
            resp.success = False
            resp.error_message = ex
        return resp

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns
        -------
        StatusResponse
            Status confirmation
        """
        return self.connect()

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
        ast = parse_sql(query)
        return self.query(ast)
