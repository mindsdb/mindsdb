import json

import shopify
import requests
import pandas as pd

from mindsdb.integrations.handlers.shopify_handler.shopify_tables import (
    ProductsTable,
    ProductVariantsTable,
    CustomersTable,
    OrdersTable,
    MarketingEventsTable,
    InventoryItemsTable,
    StaffMembersTable,
    GiftCardsTable,
    CollectionsTable,
    FulfillmentOrdersTable,
    LocationsTable,
    DraftOrdersTable,
    InventoryLevelsTable,
    TransactionsTable,
    RefundsTable,
    DiscountCodesTable,
    PagesTable,
    BlogsTable,
    ArticlesTable,
    ShopTable,
)
from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler_exceptions import (
    InvalidNativeQuery,
    ConnectionFailed,
    MissingConnectionParams,
)

logger = log.getLogger(__name__)


class ShopifyHandler(MetaAPIHandler):
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

        if not connection_data.get("shop_url"):
            raise MissingConnectionParams("Required parameter 'shop_url' is missing.")

        self.connection_data = connection_data
        self.kwargs = kwargs

        has_token = bool(connection_data.get("access_token"))
        has_oauth = bool(connection_data.get("client_id") and connection_data.get("client_secret"))
        if not has_token and not has_oauth:
            raise MissingConnectionParams(
                "Shopify connection requires either 'access_token' or both 'client_id' and 'client_secret'."
            )

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
        # Tier 1 new tables
        self._register_table("collections", CollectionsTable(self))
        self._register_table("fulfillment_orders", FulfillmentOrdersTable(self))
        self._register_table("locations", LocationsTable(self))
        self._register_table("draft_orders", DraftOrdersTable(self))
        self._register_table("inventory_levels", InventoryLevelsTable(self))
        self._register_table("transactions", TransactionsTable(self))
        self._register_table("refunds", RefundsTable(self))
        # Tier 2 new tables
        self._register_table("discount_codes", DiscountCodesTable(self))
        self._register_table("pages", PagesTable(self))
        self._register_table("blogs", BlogsTable(self))
        self._register_table("articles", ArticlesTable(self))
        self._register_table("shop", ShopTable(self))

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
        access_token = self.connection_data.get("access_token")

        if not access_token:
            client_id = self.connection_data["client_id"]
            client_secret = self.connection_data["client_secret"]

            response = requests.post(
                f"https://{shop_url}/admin/oauth/access_token",
                data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()
            access_token = result.get("access_token")
            if not access_token:
                raise ConnectionFailed("Unable to get an access token from Shopify OAuth endpoint")

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

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> Response:
        """process a raw query

        Args:
            query (str): query in a native format (graphql)
        Returns:
            Response: The query result.
        """
        api_session = self.connect()
        shopify.ShopifyResource.activate_session(api_session)
        try:
            result = shopify.GraphQL().execute(query)
        except Exception as e:
            raise InvalidNativeQuery(f"An error occurred when executing the query: {e}")

        try:
            result = json.loads(result)
            data = result.get("data")
            df = pd.DataFrame(data)
        except Exception as e:
            raise InvalidNativeQuery(f"An error occurred when parsing the query result into a DataFrame: {e}")

        return Response(RESPONSE_TYPE.TABLE, data_frame=df)
