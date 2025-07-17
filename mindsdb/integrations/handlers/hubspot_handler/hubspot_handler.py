from hubspot import HubSpot

from mindsdb.integrations.handlers.hubspot_handler.hubspot_tables import (
    ContactsTable, CompaniesTable, DealsTable
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


class HubspotHandler(APIHandler):
    """
        A class for handling connections and interactions with the Hubspot API.
    """

    name = 'hubspot'

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

        companies_data = CompaniesTable(self)
        self._register_table("companies", companies_data)

        contacts_data = ContactsTable(self)
        self._register_table("contacts", contacts_data)

        deals_data = DealsTable(self)
        self._register_table("deals", deals_data)

    def connect(self) -> HubSpot:
        """Creates a new Hubspot API client if needed and sets it as the client to use for requests.

        Returns newly created Hubspot API client, or current client if already set.
        """
        if self.is_connected is True:
            return self.connection

        access_token = self.connection_data['access_token']

        self.connection = HubSpot(access_token=access_token)
        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """Checks whether the API client is connected to Hubspot.

        Returns:
            StatusResponse: A status response indicating whether the API client is connected to Hubspot.
        """

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Hubspot: {e}')
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
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
