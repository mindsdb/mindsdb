import stripe
from mindsdb.integrations.handlers.stripe_handler.stripe_tables import CustomersTable, ProductsTable, PaymentIntentsTable, RefundsTable, PayoutsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


class StripeHandler(APIHandler):
    """
    The Stripe handler implementation.
    """

    name = 'stripe'

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

        customers_data = CustomersTable(self)
        self._register_table("customers", customers_data)

        products_data = ProductsTable(self)
        self._register_table("products", products_data)

        payment_intents_data = PaymentIntentsTable(self)
        self._register_table("payment_intents", payment_intents_data)

        payouts_data = PayoutsTable(self)
        self._register_table("payouts", payouts_data)

        refunds_data = RefundsTable(self)
        self._register_table("refunds", refunds_data)

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

        stripe.api_key = self.connection_data['api_key']

        self.connection = stripe
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
            stripe = self.connect()
            stripe.Account.retrieve()
            response.success = True
        except Exception as e:
            logger.error('Error connecting to Stripe!')
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
