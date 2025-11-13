from http import HTTPStatus

import requests
import shopify

from mindsdb.integrations.handlers.shopify_handler.shopify_tables import (
    ProductsTable,
    ProductVariantsTable,
    CustomersTable,
    OrdersTable,
    MarketingEventsTable,
    InventoryItemsTable,
    StaffMembersTable,
    GiftCardsTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql
from mindsdb.integrations.libs.api_handler_exceptions import (
    InvalidNativeQuery,
    ConnectionFailed,
    MissingConnectionParams,
)

from .connection_args import connection_args

logger = log.getLogger(__name__)


class ShopifyHandler(APIHandler):
    """
    The Shopify handler implementation.
    """

    name = "shopify"

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        if kwargs.get("connection_data") is None:
            raise MissingConnectionParams("Incomplete parameters passed to Shopify Handler")

        connection_data = kwargs.get("connection_data", {})

        required_args = [arg_name for arg_name, arg_meta in connection_args.items() if arg_meta.get('required') is True]
        missed_args = set(required_args) - set(connection_data)
        if missed_args:
            raise MissingConnectionParams(f"Required parameters are not found in the connection data: {', '.join(list(missed_args))}")

        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        self._register_table("products", ProductsTable(self))
        self._register_table("customers", CustomersTable(self))
        self._register_table("orders", OrdersTable(self))
        self._register_table("product_variants", ProductVariantsTable(self))
        self._register_table("marketing_events", MarketingEventsTable(self))
        self._register_table("inventory_items", InventoryItemsTable(self))
        self._register_table("staff_members", StaffMembersTable(self))
        self._register_table("gift_cards", GiftCardsTable(self))

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

        shop_url = self.connection_data["shop_url"]
        client_id = self.connection_data["client_id"]
        client_secret = self.connection_data["client_secret"]

        response = requests.post(
            f"https://{shop_url}/admin/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )
        response.raise_for_status()
        result = response.json()
        access_token = result.get('access_token')

        api_session = shopify.Session(shop_url, "2025-10", access_token)

        self.connection = api_session
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
            api_session = self.connect()
            shopify.ShopifyResource.activate_session(api_session)
            shopify.Shop.current()
            response.success = True
        except Exception as e:
            logger.error("Error connecting to Shopify!")
            response.error_message = str(e)
            raise ConnectionFailed("Conenction to Shopify failed.")

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
        try:
            ast = parse_sql(query)
        except Exception:
            raise InvalidNativeQuery(f"The query {query} is invalid.")
        return self.query(ast)
