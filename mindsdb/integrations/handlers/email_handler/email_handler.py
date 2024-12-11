from mindsdb.utilities import log

from mindsdb.integrations.libs.api_handler import APIHandler

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.email_handler.email_tables import EmailsTable
from mindsdb.integrations.handlers.email_handler.email_client import EmailClient
from mindsdb.integrations.handlers.email_handler.settings import EmailConnectionDetails


logger = log.getLogger(__name__)


class EmailHandler(APIHandler):
    """
    A class for handling connections and interactions with Email (send and search).

    Parameters
    ----------
    name : str
        The name of the handler
    connection_data : EmailConnectionDetails
        The connection details for the email server

        see `EmailConnectionDetails` for more details and examples

    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = EmailConnectionDetails(**connection_data)
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        emails = EmailsTable(self)
        self._register_table('emails', emails)

    def connect(self):
        """Authenticate with the email servers using credentials."""

        if self.is_connected is True:
            return self.connection

        try:
            self.connection = EmailClient(self.connection_data)
        except Exception as e:
            logger.error(f'Error connecting to email api: {e}!')
            raise e

        self.is_connected = True
        return self.connection

    def disconnect(self):
        """ Close any existing connections

        Should switch self.is_connected.
        """
        self.is_connected = False

        return self.connection.logout()

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Email: {e}. '
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

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
