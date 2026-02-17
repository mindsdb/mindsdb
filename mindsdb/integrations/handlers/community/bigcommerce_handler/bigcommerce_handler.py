from typing import Any

from mindsdb.integrations.handlers.bigcommerce_handler.bigcommerce_api_client import BigCommerceAPIClient
from mindsdb.integrations.handlers.bigcommerce_handler.bigcommerce_tables import (
    BigCommerceOrdersTable,
    BigCommerceProductsTable,
    BigCommerceCustomersTable,
    BigCommerceCategoriesTable,
    BigCommercePickupsTable,
    BigCommercePromotionsTable,
    BigCommerceWishlistsTable,
    BigCommerceSegmentsTable,
    BigCommerceBrandsTable,
)
from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class BigCommerceHandler(MetaAPIHandler):
    """This handler handles the connection and execution of SQL statements on BigCommerce."""

    name = "bigcommerce"

    def __init__(self, name: str, connection_data: dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (str): The name of the handler instance.
            connection_data (dict): The connection data required to connect to the BigCommerce API.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.thread_safe = True

        self._register_table("orders", BigCommerceOrdersTable(self))
        self._register_table("products", BigCommerceProductsTable(self))
        self._register_table("customers", BigCommerceCustomersTable(self))
        self._register_table("categories", BigCommerceCategoriesTable(self))
        self._register_table("pickups", BigCommercePickupsTable(self))
        self._register_table("promotions", BigCommercePromotionsTable(self))
        self._register_table("wishlists", BigCommerceWishlistsTable(self))
        self._register_table("segments", BigCommerceSegmentsTable(self))
        self._register_table("brands", BigCommerceBrandsTable(self))

    def connect(self) -> BigCommerceAPIClient:
        """
        Establishes a connection to the BigCommerce API.

        Raises:
            ValueError: If the required connection parameters are not provided.

        Returns:
            BigCommerceAPIClient: A connection object to the BigCommerce API.
        """
        if self.is_connected is True:
            return self.connection

        if not all(
            key in self.connection_data and self.connection_data.get(key) for key in ["api_base", "access_token"]
        ):
            raise ValueError("Required parameters (api_base, access_token) must be provided and should not be empty.")

        self.connection = BigCommerceAPIClient(
            url=self.connection_data.get("api_base"),
            access_token=self.connection_data.get("access_token"),
        )

        self.is_connected = True
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the BigCommerce API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            connection.get_products(limit=1)
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to BigCommerce API: {e}!")
            response.error_message = e

        self.is_connected = response.success

        return response
