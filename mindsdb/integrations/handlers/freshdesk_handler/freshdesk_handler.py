from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.freshdesk_handler.freshdesk_tables import FreshdeskAgentsTable, FreshdeskTicketsTable

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log
from freshdesk.v2.api import API

logger = log.getLogger(__name__)


class FreshdeskHandler(APIHandler):
    """The Freshdesk handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the freshdesk handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.freshdesk_client: API = None
        self.is_connected = False

        self._register_table("agents", FreshdeskAgentsTable(self))
        self._register_table("tickets", FreshdeskTicketsTable(self))

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        try:
            if not self.connection_data.get("domain"):
                raise ValueError("Missing required parameter: domain")
            if not self.connection_data.get("api_key"):
                raise ValueError("Missing required parameter: api_key")

            self.freshdesk_client = API(
                domain=self.connection_data["domain"],
                api_key=self.connection_data["api_key"]
            )
            # Test the connection by getting new tickets
            self.freshdesk_client.tickets.list_new_and_my_open_tickets(
                page=1, per_page=1)
            self.is_connected = True
            resp.success = True
        except KeyError as ex:
            resp.success = False
            resp.error_message = f"Missing required connection parameter: {str(ex)}"
            self.is_connected = False
        except ValueError as ex:
            resp.success = False
            resp.error_message = str(ex)
            self.is_connected = False
        except Exception as ex:
            resp.success = False
            resp.error_message = f"Failed to connect to Freshdesk: {str(ex)}"
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
