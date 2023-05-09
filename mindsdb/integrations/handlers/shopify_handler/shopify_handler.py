import shopify

from mindsdb.integrations.handlers.shopify_handler.shopify_tables import ProductsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql import parse_sql


class ShopifyHandler(APIHandler):
    """
    The Shopify handler implementation.
    """

    name = 'shopify'

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

        products_data = ProductsTable(self)
        self._register_table("products", products_data)

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

        api_session = shopify.Session(self.connection_data['shop_url'], '2021-10', self.connection_data['access_token'])
        # shopify.ShopifyResource.activate_session(api_session)

        self.connection = api_session

        self.is_connected = True

        return self.connection