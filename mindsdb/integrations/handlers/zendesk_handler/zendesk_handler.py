from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.zendesk_handler.zendesk_tables import (
    ZendeskUsersTable,
    ZendeskTicketsTable,
    ZendeskTriggersTable,
    ZendeskActivitiesTable
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log
import zenpy

logger = log.getLogger(__name__)


class ZendeskHandler(APIHandler):
    """The Zendesk handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the zendesk handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.zen_client = None
        self.is_connected = False

        self._register_table("users", ZendeskUsersTable(self))
        self._register_table("tickets", ZendeskTicketsTable(self))
        self._register_table("triggers", ZendeskTriggersTable(self))
        self._register_table("activities", ZendeskActivitiesTable(self))

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        self.zen_client = zenpy.Zenpy(subdomain=self.connection_data["sub_domain"], email=self.connection_data["email"], token=self.connection_data["api_key"])
        try:
            self.zen_client.users()
            self.is_connected = True
            resp.success = True
        except Exception as ex:
            resp.success = False
            resp.error_message = str(ex)
            self.is_connected = False
        return resp

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = self.connect()
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
        ast = parse_sql(query)
        return self.query(ast)
