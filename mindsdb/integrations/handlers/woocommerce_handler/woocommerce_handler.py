from woocommerce import API

from mindsdb.integrations.handlers.woocommerce_handler.woocommerce_tables import *
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.api_handler_exceptions import InvalidNativeQuery, ConnectionFailed, MissingConnectionParams

logger = log.getLogger(__name__)

class WoocommerceHandler(APIHandler):
    """
    The woocommerce handler implementation.
    """

    name = 'woocommerce'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        if kwargs.get("connection_data") is None:
            raise MissingConnectionParams(f"Incomplete parameters passed to woocommerce Handler")

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

        tax_data = TaxesTable(self)
        self._register_table("taxes", tax_data)

        setting_data = SettingsTable(self)
        self._register_table("settings", setting_data)

        coupon_reviews_data = CouponsTable(self)
        self._register_table("coupons", coupon_reviews_data)

        shipping_zone_data = ShippingZonesTable(self)
        self._register_table("shipping_zones", shipping_zone_data)

        payment_gateway_data = PaymentGatewaysTable(self)
        self._register_table("payment_gateways", payment_gateway_data)

        report_sale_data = ReportSalesTable(self)
        self._register_table("report_sales", report_sale_data)

        report_top_seller_data = ReportTopSellersTable(self)
        self._register_table("report_top_sellers", report_top_seller_data)
        
        setting_option_data = SettingOptionTable(self)
        self._register_table("setting_options", setting_option_data)
        
        # draft_orders_data = DraftOrdersTable(self)
        # self._register_table("draft_orders", draft_orders_data)
        
        # checkouts_data = CheckoutTable(self)
        # self._register_table("checkouts", checkouts_data)
        
        # price_rule_data = PriceRuleTable(self)
        # self._register_table("price_rules", price_rule_data)
        
        refund_data = RefundsTable(self)
        self._register_table("refunds", refund_data)
        
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
        
        # shop_data = ShopTable(self)
        # self._register_table("shop", shop_data)
        
        # user_data = UserTable(self)
        # self._register_table("user", user_data)
        
        # gift_card_data = GiftCardTable(self)
        # self._register_table("gift_cards", gift_card_data)
        
        
        # resource_feedback_data = ResourceFeedbackTable(self)
        # self._register_table("resource_feedbacks", resource_feedback_data)
        
        # order_risk_data = OrderRiskTable(self)
        # self._register_table("order_risks", order_risk_data)
        
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
            raise MissingConnectionParams(f"Incomplete parameters passed to woocommerce Handler")

        wcapi = API(self.connection_data['url'], self.connection_data['consumer_key'], self.connection_data['consumer_secret'])

        self.connection = wcapi
        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        
        wcapi = self.connect()
        
        res = wcapi.get('products')
        logger.info(res.status_code)
        if res.status_code == 200:
            response.success = True
        else:
            response.success = False
            logger.error(f'Error connecting to Woocommerce!')
            raise ConnectionFailed(f"Conenction to Woocommerce failed.")
            response.error_message = res.text
            
            
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