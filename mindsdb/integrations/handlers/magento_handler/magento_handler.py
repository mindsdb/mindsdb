import magento
from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.magento_handler.magento_tables import (
    CustomersTable,
    InvoicesTable,
    OrdersTable,
    ProductsTable,
    CategoriesTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.api_handler_exceptions import (
    ConnectionFailed,
    InvalidNativeQuery,
    MissingConnectionParams,
)
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MagentoHandler(APIHandler):
    """
    The magento handler implementation.
    """

    name = "magento"

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        if kwargs.get("connection_data") is None:
            raise MissingConnectionParams(
                f"Incomplete parameters passed to magento Handler"
            )

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

        invoice_data = InvoicesTable(self)
        self._register_table("invoices", invoice_data)

        category_data = CategoriesTable(self)
        self._register_table("categories", category_data)

        # customer_reviews_data = CustomerReviews(self)
        # self._register_table("customer_reviews", customer_reviews_data)

        # carrier_service_data = CarrierServiceTable(self)
        # self._register_table("carrier_service", carrier_service_data)

        # shipping_zone_data = ShippingZoneTable(self)
        # self._register_table("shipping_zone", shipping_zone_data)

        # sales_channel_data = SalesChannelTable(self)
        # self._register_table("sales_channel", sales_channel_data)

        # smart_collections_data = SmartCollectionsTable(self)
        # self._register_table("smart_collections", smart_collections_data)

        # custom_collections_data = CustomCollectionsTable(self)
        # self._register_table("custom_collections", custom_collections_data)

        # draft_orders_data = DraftOrdersTable(self)
        # self._register_table("draft_orders", draft_orders_data)

        # checkouts_data = CheckoutTable(self)
        # self._register_table("checkouts", checkouts_data)

        # price_rule_data = PriceRuleTable(self)
        # self._register_table("price_rules", price_rule_data)

        # refund_data = RefundsTable(self)
        # self._register_table("refunds", refund_data)

        # discount_data = DiscountCodesTable(self)
        # self._register_table("discounts", discount_data)

        # marketing_event_data = MarketingEventTable(self)
        # self._register_table("marketing_events", marketing_event_data)

        # blog_data = BlogTable(self)
        # self._register_table("blogs", blog_data)

        # theme_data = ThemeTable(self)
        # self._register_table("themes", theme_data)

        # article_data = ArticleTable(self)
        # self._register_table("articles", article_data)

        # comment_data = CommentTable(self)
        # self._register_table("comments", comment_data)

        # page_data = PageTable(self)
        # self._register_table("pages", page_data)

        # redirect_data = redirectTable(self)
        # self._register_table("redirects", redirect_data)

        # tender_transaction_data = TenderTransactionTable(self)
        # self._register_table("tender_transactions", tender_transaction_data)

        # policy_data = PolicyTable(self)
        # self._register_table("policies", policy_data)

        # user_data = UserTable(self)
        # self._register_table("user", user_data)

        # gift_card_data = GiftCardTable(self)
        # self._register_table("gift_cards", gift_card_data)

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
            raise MissingConnectionParams(
                f"Incomplete parameters passed to Magento Handler"
            )

        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36"

        if "user_agent" in self.connection_data:
            user_agent = self.connection_data["user_agent"]

        api = magento.Client(
            domain=self.connection_data["domain"],
            username=self.connection_data["username"],
            password=self.connection_data["password"],
            user_agent=user_agent,
        )

        self.connection = api

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
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(e)
            raise ConnectionFailed("Conenction to magento store failed.")

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
            ast = parse_sql(query, dialect="mindsdb")
        except Exception as e:
            raise InvalidNativeQuery(f"The query {query} is invalid.")
        return self.query(ast)
