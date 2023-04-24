import sib_api_v3_sdk

from mindsdb.integrations.handlers.sendinblue_handler.sendinblue_tables import EmailCampaignsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql


logger = get_log("integrations.sendinblue_handler")


class SendinblueHandler(APIHandler):
    """
    The Sendinblue handler implementation.
    """

    name = 'sendinblue'

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

        email_campaigns_data = EmailCampaignsTable(self)
        self._register_table("email_campaigns", email_campaigns_data)

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection

        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = self.connection_data['api_key']

        self.connection = configuration

        self.is_connected = True

        return self.connection