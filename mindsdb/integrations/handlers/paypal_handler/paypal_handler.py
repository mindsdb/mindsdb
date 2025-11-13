import paypalrestsdk

from mindsdb.integrations.handlers.paypal_handler.paypal_tables import (
    InvoicesTable,
    PaymentsTable,
    SubscriptionsTable,
    OrdersTable,
    PayoutsTable
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb_sql_parser import parse_sql
from collections import OrderedDict

logger = log.getLogger(__name__)


class PayPalHandler(APIHandler):
    """
    The PayPal handler implementation.
    """

    name = 'paypal'

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

        payments_data = PaymentsTable(self)
        self._register_table("payments", payments_data)

        invoices_data = InvoicesTable(self)
        self._register_table("invoices", invoices_data)

        subscriptions_data = SubscriptionsTable(self)
        self._register_table("subscriptions", subscriptions_data)

        orders_data = OrdersTable(self)
        self._register_table("orders", orders_data)

        payouts_data = PayoutsTable(self)
        self._register_table("payouts", payouts_data)

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

        self.connection = paypalrestsdk.Api(
            {
                "mode": self.connection_data['mode'],
                "client_id": self.connection_data['client_id'],
                "client_secret": self.connection_data['client_secret'],
            }
        )

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
            connection.get_access_token()
            response.success = True
        except Exception as e:
            logger.error('Error connecting to PayPal!')
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


connection_args = OrderedDict(
    mode={
        "type": ARG_TYPE.STR,
        "description": "Environment mode of the app",
        "required": True,
        "label": "MODE",
    },
    client_id={
        "type": ARG_TYPE.PWD,
        "description": "Client id of the App",
        "required": True,
        "label": "Client ID",
    },
    client_secret={
        "type": ARG_TYPE.STR,
        "description": "Client secret of the App",
        "required": True,
        "label": "Client Secret",
    },
)

connection_args_example = OrderedDict(
    mode="sandbox",
    client_id="xxxx-xxxx-xxxx-xxxx",
    client_secret="<secret>",
)
