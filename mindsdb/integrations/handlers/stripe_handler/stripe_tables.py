import pandas as pd
import stripe
from typing import Text, List, Dict, Any

from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import INSERTQueryParser, DELETEQueryParser, UPDATEQueryParser, DELETEQueryExecutor, UPDATEQueryExecutor
from mindsdb.integrations.utilities.handlers.query_utilities.select_query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class CustomersTable(APITable):
    """The Stripe Customers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Stripe Customer data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Stripe Customers matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

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
        stripe = self.handler.connect()
        customers = stripe.Customer.list(**kwargs)
        return [customer.to_dict() for customer in customers]


class ProductsTable(APITable):
    """The Stripe Products Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Stripe Product data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Stripe Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

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

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into Stripe "POST v1/products" API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=['id', 'name', 'active', 'description', 'metadata'],
            mandatory_columns=['name'],
            all_mandatory=False,
        )
        product_data = insert_statement_parser.parse_query()
        self.create_products(product_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from Stripe "POST v1/products/:id" API endpoint.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        products_df = pd.json_normalize(self.get_products())
        update_query_executor = UPDATEQueryExecutor(
            products_df,
            where_conditions
        )

        products_df = update_query_executor.execute_query()
        product_ids = products_df['id'].tolist()
        self.update_products(product_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from Stripe "DELETE v1/products/:id" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        products_df = pd.json_normalize(self.get_products())
        delete_query_executor = DELETEQueryExecutor(
            products_df,
            where_conditions
        )

        products_df = delete_query_executor.execute_query()
        product_ids = products_df['id'].tolist()
        self.delete_products(product_ids)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_products(limit=1)).columns.tolist()

    def get_products(self, **kwargs) -> List[Dict]:
        stripe = self.handler.connect()
        products = stripe.Product.list(**kwargs)
        return [product.to_dict() for product in products]

    def create_products(self, product_data: List[Dict[Text, Any]]) -> None:
        stripe = self.handler.connect()
        for product in product_data:
            created_product = stripe.Product.create(**product)
            if 'id' not in created_product.to_dict():
                raise Exception('Product creation failed')
            else:
                logger.info(f'Product {created_product.to_dict()["id"]} created')

    def update_products(self, product_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        stripe = self.handler.connect()
        for product_id in product_ids:
            updated_product = stripe.Product.modify(product_id, **values_to_update)
            if 'id' not in updated_product.to_dict():
                raise Exception('Product update failed')
            else:
                logger.info(f'Product {updated_product.to_dict()["id"]} updated')

    def delete_products(self, product_ids: List[Text]) -> None:
        stripe = self.handler.connect()
        for product_id in product_ids:
            deleted_product = stripe.Product.delete(product_id)
            if 'id' not in deleted_product.to_dict():
                raise Exception('Product deletion failed')
            else:
                logger.info(f'Product {deleted_product.to_dict()["id"]} deleted')


class PaymentIntentsTable(APITable):
    """The Stripe Payment Intents Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Stripe Payment Intents data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Stripe Payment Intents matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

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

    def delete(self, query: ast.Delete) -> None:
        """
        Cancels Stripe Payment Intents and updates the local data.

        Parameters
        ----------
        query : ast.Delete
        Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
        If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        if 'payment_intents_df' not in self.__dict__:
            self.payment_intents_df = pd.json_normalize(self.get_payment_intents())

        delete_query_executor = DELETEQueryExecutor(
            self.payment_intents_df,
            where_conditions
        )

        canceled_payment_intents_df = delete_query_executor.execute_query()

        payment_intent_ids = canceled_payment_intents_df['id'].tolist()
        self.cancel_payment_intents(payment_intent_ids)

        self.payment_intents_df = self.payment_intents_df[~self.payment_intents_df['id'].isin(payment_intent_ids)]

    def cancel_payment_intents(self, payment_intent_ids: List[str]) -> None:
        stripe = self.handler.connect()
        for payment_intent_id in payment_intent_ids:
            try:

                payment_intent = stripe.PaymentIntent.retrieve(payment_intent_id)
                if payment_intent.status in ['requires_payment_method', 'requires_capture', 'requires_confirmation', 'requires_action', 'processing']:
                    stripe.PaymentIntent.cancel(payment_intent_id)
                else:
                    logger.warning(f"Payment intent {payment_intent_id} is in status {payment_intent.status} and cannot be canceled.")
            except stripe.error.StripeError as e:
                logger.error(f"Error cancelling payment intent {payment_intent_id}: {str(e)}")

    def update(self, query: 'ast.Update') -> None:
        """
        Updates data in Stripe "POST /v1/payment_intents/:id" API endpoint.

        Parameters
        ----------
        query : ast.Update
            Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        payment_intents_df = pd.json_normalize(self.get_payment_intents())
        update_query_executor = UPDATEQueryExecutor(
            payment_intents_df,
            where_conditions
        )

        payment_intents_df = update_query_executor.execute_query()
        payment_intent_ids = payment_intents_df['id'].tolist()
        self.update_payment_intents(payment_intent_ids, values_to_update)

    def update_payment_intents(self, payment_intent_ids: list, values_to_update: dict) -> None:
        for payment_intent_id in payment_intent_ids:
            stripe.PaymentIntent.modify(payment_intent_id, **values_to_update)

    def insert(self, query: 'ast.Insert') -> None:
        """
        Inserts data into Stripe "POST /v1/payment_intents" API endpoint.

        Parameters
        ----------
        query : ast.Insert
            Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=['amount', 'currency', 'description', 'payment_method_types'],
            mandatory_columns=['amount', 'currency'],
            all_mandatory=True
        )
        payment_intent_data = insert_statement_parser.parse_query()
        self.create_payment_intent(payment_intent_data)

    def create_payment_intent(self, payment_intent_data: list) -> None:
        for data in payment_intent_data:
            stripe.PaymentIntent.create(**data)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_payment_intents(limit=1)).columns.tolist()

    def get_payment_intents(self, **kwargs) -> List[Dict]:
        stripe = self.handler.connect()
        payment_intents = stripe.PaymentIntent.list(**kwargs)
        return [payment_intent.to_dict() for payment_intent in payment_intents]


class RefundsTable(APITable):
    """The Stripe Refund Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Stripe Refund data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Stripe Refunds matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'refunds',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        refunds_df = pd.json_normalize(self.get_refunds(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            refunds_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        refunds_df = select_statement_executor.execute_query()

        return refunds_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_refunds(limit=1)).columns.tolist()

    def get_refunds(self, **kwargs) -> List[Dict]:
        stripe = self.handler.connect()
        refunds = stripe.Refund.list(**kwargs)
        return [refund.to_dict() for refund in refunds]


class PayoutsTable(APITable):
    """The Stripe Payouts Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Stripe Payout data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Stripe Payouts matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'payouts',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        payouts_df = pd.json_normalize(self.get_payouts(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            payouts_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        payouts_df = select_statement_executor.execute_query()

        return payouts_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_payouts(limit=1)).columns.tolist()

    def get_payouts(self, **kwargs) -> List[Dict]:
        stripe = self.handler.connect()
        payouts = stripe.Payout.list(**kwargs)
        return [payout.to_dict() for payout in payouts]
