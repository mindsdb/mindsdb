import sib_api_v3_sdk

from mindsdb.integrations.handlers.sendinblue_handler.sendinblue_tables import EmailCampaignsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


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

        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = self.connection_data['api_key']

        self.connection = sib_api_v3_sdk.ApiClient(configuration)

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
            connection = self.connect()
            api_instance = sib_api_v3_sdk.AccountApi(connection)
            api_instance.get_account()
            response.success = True
        except Exception as e:
            logger.error('Error connecting to Sendinblue!')
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
        ast = parse_sql(query)
        return self.query(ast)
