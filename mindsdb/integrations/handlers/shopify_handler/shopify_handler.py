import shopify
import requests
import re

from mindsdb.integrations.handlers.shopify_handler.shopify_tables import (
    ProductsTable,
    CustomersTable,
    OrdersTable,
    InventoryLevelTable,
    LocationTable,
    CustomerReviews,
    CarrierServiceTable,
    ShippingZoneTable,
    SalesChannelTable,
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
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        products_data = ProductsTable(self)
        self._register_table("products", products_data)

        customers_data = CustomersTable(self)
        self._register_table("customers", customers_data)

        orders_data = OrdersTable(self)
        self._register_table("orders", orders_data)

        inventory_level_data = InventoryLevelTable(self)
        self._register_table("inventory_level", inventory_level_data)

        location_data = LocationTable(self)
        self._register_table("locations", location_data)

        customer_reviews_data = CustomerReviews(self)
        self._register_table("customer_reviews", customer_reviews_data)

        carrier_service_data = CarrierServiceTable(self)
        self._register_table("carrier_service", carrier_service_data)

        shipping_zone_data = ShippingZoneTable(self)
        self._register_table("shipping_zone", shipping_zone_data)

        sales_channel_data = SalesChannelTable(self)
        self._register_table("sales_channel", sales_channel_data)

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

        if self.kwargs.get("connection_data") is None:
            raise MissingConnectionParams("Incomplete parameters passed to Shopify Handler")

        api_session = shopify.Session(
            self.connection_data["shop_url"].strip(), "2021-10", self.connection_data["access_token"]
        )

        self.yotpo_app_key = self.connection_data["yotpo_app_key"] if "yotpo_app_key" in self.connection_data else None
        self.yotpo_access_token = (
            self.connection_data["yotpo_access_token"] if "yotpo_access_token" in self.connection_data else None
        )

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
            logger.error(f"Error connecting to Shopify: {str(e)}")
            response.error_message = str(e)

            # Parse the error to extract clean, user-friendly messages
            error_msg = str(e)

            # Handle ClientError (Shopify API errors)
            if "ClientError" in error_msg and "Response(" in error_msg:
                # Extract HTTP status code
                if "code=402" in error_msg:
                    if '"errors":"Unavailable Shop"' in error_msg or "Unavailable Shop" in error_msg:
                        raise ConnectionFailed(
                            "Shopify shop is unavailable. This could be due to shop suspension, billing issues, or incorrect shop URL."
                        )
                    else:
                        raise ConnectionFailed(
                            "Shopify API access requires payment. Please check your Shopify billing status."
                        )
                elif "code=401" in error_msg:
                    raise ConnectionFailed(
                        "Invalid Shopify API credentials. Please check your access token and shop URL."
                    )
                elif "code=404" in error_msg:
                    raise ConnectionFailed("Shopify shop not found. Please verify the shop URL is correct.")
                elif "code=403" in error_msg:
                    raise ConnectionFailed("Access denied. Please check your API permissions and credentials.")
                else:
                    body_match = re.search(r'"errors":"([^"]+)"', error_msg)
                    if body_match:
                        shopify_error = body_match.group(1)
                        raise ConnectionFailed(f"Shopify API error: {shopify_error}")
                    else:
                        raise ConnectionFailed(
                            "Failed to connect to Shopify API. Please check your credentials and shop URL."
                        )

            # Handle other HTTP errors
            elif "HTTP Error 402" in error_msg:
                raise ConnectionFailed(
                    "Shopify shop is unavailable or requires payment. Please check your shop status and billing."
                )
            elif "HTTP Error 401" in error_msg or "Unauthorized" in error_msg:
                raise ConnectionFailed("Invalid Shopify API credentials. Please check your access token and shop URL.")
            elif "HTTP Error 404" in error_msg:
                raise ConnectionFailed("Shopify shop not found. Please verify the shop URL is correct.")
            elif "ConnectionError" in error_msg or "timeout" in error_msg.lower():
                raise ConnectionFailed(
                    "Network connection failed. Please check your internet connection and try again."
                )
            else:
                # Generic error
                raise ConnectionFailed("Failed to connect to Shopify. Please verify your shop URL and access token.")

            response.success = False

        if self.yotpo_app_key is not None and self.yotpo_access_token is not None:
            url = f"https://api.yotpo.com/v1/apps/{self.yotpo_app_key}/reviews?count=1&utoken={self.yotpo_access_token}"
            headers = {"accept": "application/json", "Content-Type": "application/json"}
            if requests.get(url, headers=headers).status_code == 200:
                response.success = True
            else:
                response.success = False

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
