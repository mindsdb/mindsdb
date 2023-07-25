from mindsdb.utilities import log

from mindsdb.integrations.libs.api_handler import APIHandler

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.email_handler.email_tables import EmailsTable
from mindsdb.integrations.handlers.email_handler.email_helpers import EmailClient


class EmailHandler(APIHandler):
    """A class for handling connections and interactions with Email (send and search).

    Attributes:
        "email": "Email address used to login to the SMTP and IMAP servers.",
        "password": "Password used to login to the SMTP and IMAP servers.",
        "smtp_server": "SMTP server to be used for sending emails. Default value is 'smtp.gmail.com'.",
        "smtp_port": "Port number for the SMTP server. Default value is 587.",
        "imap_server": "IMAP server to be used for reading emails. Default value is 'imap.gmail.com'."
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
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
            self.connection = EmailClient(**self.connection_data)
        except Exception as e:
            log.logger.error(f'Error connecting to email api: {e}!')
            raise e

        self.is_connected = True
        return self.connection

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            connection = self.connect()
            # TODO: check connection

            response.success = True

        except Exception as e:
            log.logger.error(f'Error connecting to email api: {e}!')
            response.error_message = e

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
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)

