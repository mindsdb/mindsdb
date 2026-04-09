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
    AnalyticsTable,
    AbandonedCheckoutsTable,
    DeliveryProfilesTable,
    CarrierServicesTable,
    MarketsTable,
    CompaniesTable,
    CompanyLocationsTable,
    CompanyContactsTable,
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
        self.handler_storage = kwargs.get("handler_storage")
        self.kwargs = kwargs

        has_token = bool(connection_data.get("access_token"))
        has_refresh = bool(
            connection_data.get("refresh_token")
            and connection_data.get("client_id")
            and connection_data.get("client_secret")
        )
        has_oauth = bool(connection_data.get("client_id") and connection_data.get("client_secret"))
        if not has_token and not has_refresh and not has_oauth:
            raise MissingConnectionParams(
                "Shopify connection requires 'access_token', or 'refresh_token'+'client_id'+'client_secret'."
            )

        self.connection = None
        self.is_connected = False

        self._register_table("products", ProductsTable(self))
        self._register_table("customers", CustomersTable(self))
        self._register_table("orders", OrdersTable(self))
        self._register_table("product_variants", ProductVariantsTable(self))
        self._register_table("marketing_events", MarketingEventsTable(self))
        self._register_table("inventory_items", InventoryItemsTable(self))

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
        # New read scope tables
        self._register_table("analytics", AnalyticsTable(self))
        self._register_table("abandoned_checkouts", AbandonedCheckoutsTable(self))
        self._register_table("delivery_profiles", DeliveryProfilesTable(self))
        self._register_table("carrier_services", CarrierServicesTable(self))
        self._register_table("markets", MarketsTable(self))
        # B2B tables (Shopify Plus only — read_companies scope)
        self._register_table("companies", CompaniesTable(self))
        self._register_table("company_locations", CompanyLocationsTable(self))
        self._register_table("company_contacts", CompanyContactsTable(self))

    def _persist_tokens(self):
        """Persist current tokens to handler_storage so they survive restarts."""
        if not self.handler_storage:
            return
        self.handler_storage.encrypted_json_set("shopify_tokens", {
            "access_token": self.connection_data.get("access_token"),
            "refresh_token": self.connection_data.get("refresh_token"),
            "expires_at": self.connection_data.get("expires_at"),
        })

    def _load_persisted_tokens(self):
        """Load tokens from handler_storage if available. Returns dict or None."""
        if not self.handler_storage:
            return None
        try:
            return self.handler_storage.encrypted_json_get("shopify_tokens")
        except Exception:
            return None

    def _do_refresh_token(self) -> str:
        """Exchange a refresh token for a new access token (Shopify token rotation)."""
        from datetime import datetime, timezone, timedelta

        shop_url = self.connection_data["shop_url"]
        response = requests.post(
            f"https://{shop_url}/admin/oauth/access_token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.connection_data["refresh_token"],
                "client_id": self.connection_data["client_id"],
                "client_secret": self.connection_data["client_secret"],
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        response.raise_for_status()
        token_data = response.json()
        new_access_token = token_data.get("access_token")
        if not new_access_token:
            raise ConnectionFailed("Token rotation did not return an access_token")

        self.connection_data["access_token"] = new_access_token

        # Rotate refresh token (Shopify always issues a new one-time-use token)
        new_refresh_token = token_data.get("refresh_token")
        if new_refresh_token:
            self.connection_data["refresh_token"] = new_refresh_token

        # Compute new expires_at from expires_in (seconds)
        expires_in = token_data.get("expires_in")
        if expires_in:
            new_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
            self.connection_data["expires_at"] = new_expires_at.isoformat()

        # Persist to handler_storage so tokens survive restarts
        self._persist_tokens()

        return new_access_token

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

        # Load persisted tokens (survives restarts when refresh has rotated them)
        stored = self._load_persisted_tokens()
        if stored:
            for key in ("access_token", "refresh_token", "expires_at"):
                if stored.get(key):
                    self.connection_data[key] = stored[key]

        shop_url = self.connection_data["shop_url"]
        access_token = self.connection_data.get("access_token")

        # If refresh credentials are present, check expiry and rotate if needed
        if self.connection_data.get("refresh_token") and self.connection_data.get("client_id"):
            from datetime import datetime, timezone, timedelta
            expires_at_str = self.connection_data.get("expires_at")
            needs_refresh = not access_token
            if not needs_refresh and expires_at_str:
                try:
                    expires_at = datetime.fromisoformat(expires_at_str)
                    needs_refresh = expires_at < datetime.now(timezone.utc) + timedelta(minutes=5)
                except (ValueError, TypeError):
                    pass
            if needs_refresh:
                access_token = self._do_refresh_token()

        if not access_token:
            # existing client_credentials fallback (legacy path)
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
            response.copy_storage = True
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
