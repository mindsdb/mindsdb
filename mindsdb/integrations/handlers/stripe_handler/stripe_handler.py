import stripe

from mindsdb.integrations.handlers.stripe_handler.stripe_tables import CustomersTable, ProductsTable, PaymentIntentsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql import parse_sql


class YourAPIHandler:
    def connect(self):
        pass


class CustomersTable(APITable):
    """The Stripe Customers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        select_statement_parser = SELECTQueryParser(
            query,
            'customers',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        customers_df = pd.json_normalize(self.get_customers(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            customers_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        customers_df = select_statement_executor.execute_query()

        return customers_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_customers(limit=1)).columns.tolist()

    def get_customers(self, **kwargs) -> List[Dict]:
        stripe.api_key = self.handler.connection_data['api_key']
        customers = stripe.Customer.list(**kwargs)
        return [customer.to_dict() for customer in customers]

    def insert(self, query: ast.Insert) -> int:
        values = query.values
        stripe.api_key = self.handler.connection_data['api_key']

        try:
            customer = stripe.Customer.create(
                name=values['name'],
                email=values['email'],
                # Add other attributes as needed
            )
            return 1
        except stripe.error.StripeError as e:
            log.logger.error(f"Stripe Error: {e}")
            return 0

    def update(self, query: ast.Update) -> int:
        values = query.values
        conditions = query.where
        stripe.api_key = self.handler.connection_data['api_key']

        try:
            customer_id = conditions['id']
            updated_customer = stripe.Customer.modify(
                customer_id,
                name=values['name'],
                email=values['email'],
                # Add other attributes as needed
            )
            return 1
        except stripe.error.StripeError as e:
            log.logger.error(f"Stripe Error: {e}")
            return 0

    def delete(self, query: ast.Delete) -> int:
        conditions = query.where
        stripe.api_key = self.handler.connection_data['api_key']

        try:
            customer_id = conditions['id']
            deleted_customer = stripe.Customer.delete(customer_id)
            return 1
        except stripe.error.StripeError as e:
            log.logger.error(f"Stripe Error: {e}")
            return 0


class ProductsTable(APITable):
    """The Stripe Products Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        select_statement_parser = SELECTQueryParser(
            query,
            'products',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        products_df = pd.json_normalize(self.get_products(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            products_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        products_df = select_statement_executor.execute_query()

        return products_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_products(limit=1)).columns.tolist()

    def get_products(self, **kwargs) -> List[Dict]:
        stripe.api_key = self.handler.connection_data['api_key']
        products = stripe.Product.list(**kwargs)
        return [product.to_dict() for product in products]


class PaymentIntentsTable(APITable):
    """The Stripe Payment Intents Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        select_statement_parser = SELECTQueryParser(
            query,
            'payment_intents',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        payment_intents_df = pd.json_normalize(self.get_payment_intents(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            payment_intents_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        payment_intents_df = select_statement_executor.execute_query()

        return payment_intents_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_payment_intents(limit=1)).columns.tolist()

    def get_payment_intents(self, **kwargs) -> List[Dict]:
        stripe.api_key = self.handler.connection_data['api_key']
        payment_intents = stripe.PaymentIntent.list(**kwargs)
        return [payment_intent.to_dict() for payment_intent in payment_intents]


class StripeHandler(APIHandler):
    """
    The Stripe handler implementation.
    """

    name = 'stripe'

    def __init__(self, name: str, **kwargs):
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

    def connect(self):
        if self.is_connected is True:
            return self.connection

        stripe.api_key = self.connection_data['api_key']

        self.connection = stripe
        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            stripe = self.connect()
            stripe.Account.retrieve()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to Stripe!')
            response.error_message = str(e)

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> StatusResponse:
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)
