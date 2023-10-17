import paypalrestsdk
import requests

from mindsdb.integrations.handlers.paypal_handler.paypal_tables import InvoicesTable, PaymentsTable, OrdersTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql import parse_sql


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

        if 'base_url' not in self.connection_data or 'headers' not in self.connection_data:
           raise ValueError("Both 'base_url' and 'headers' must be provided in connection_data.")

        self.connection = None
        self.is_connected = False

        payments_data = PaymentsTable(self)
        self._register_table("payments", payments_data)

        invoices_data = InvoicesTable(self)
        self._register_table("invoices", invoices_data);
    
        orders_data = OrdersTable(self)
        self._register_table("orders", orders_data);


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
        
        
        BASE_URL = self.connection_data['base_url']
        HEADERS = self.connection_data['headers']

        response = requests.get(BASE_URL, headers=HEADERS)

        if response.status_code == 200:
           self.is_connected = True
        else:
           self.is_connected = False
        log.logger.error(f'Error connecting to PayPal Orders API: {response.content}')

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
            log.logger.error(f'Error connecting to PayPal!')
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
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)