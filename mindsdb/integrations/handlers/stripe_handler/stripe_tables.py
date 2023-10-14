import pandas as pd
from typing import Text, List, Dict

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities.select_query_utilities import SELECTQueryParser, SELECTQueryExecutor


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

    def insert(self, query: ast.Insert) -> int:
        """
        Inserts new customer data.

        Parameters
        ----------
        query : ast.Insert
            SQL INSERT query

        Returns
        -------
        int
            The number of rows inserted
        """
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
        """
        Updates customer data.

        Parameters
        ----------
        query : ast.Update
            SQL UPDATE query

        Returns
        -------
        int
            The number of rows updated
        """
        values = query.values
        conditions = query.where
        stripe.api_key = self.handler.connection_data['api_key']

        try:
            customer_id = conditions['id']  # Assuming 'id' is the condition for update
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
        """
        Deletes customer data.

        Parameters
        ----------
        query : ast.Delete
            SQL DELETE query

        Returns
        -------
        int
            The number of rows deleted
        """
        conditions = query.where
        stripe.api_key = self.handler.connection_data['api_key']

        try:
            customer_id = conditions['id']  # Assuming 'id' is the condition for delete
            deleted_customer = stripe.Customer.delete(customer_id)
            return 1 
        except stripe.error.StripeError as e:
            log.logger.error(f"Stripe Error: {e}")
            return 0  


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

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_products(limit=1)).columns.tolist()

    def get_products(self, **kwargs) -> List[Dict]:
        stripe = self.handler.connect()
        products = stripe.Product.list(**kwargs)
        return [product.to_dict() for product in products]

    def insert(self, query: ast.Insert) -> int:
        """
        Inserts new product data.

        Parameters
        ----------
        query : ast.Insert
            SQL INSERT query

        Returns
        -------
        int
            The number of rows inserted
        """
        values = query.values
        stripe.api_key = self.handler.connection_data['api_key']

        try:
            new_product = stripe.Product.create(
                name=values['name'],
                description=values['description'],
            )
            return 1 
        except stripe.error.StripeError as e:
            log.logger.error(f"Stripe Error: {e}")
            return 0  

    def update(self, query: ast.Update) -> int:
        """
        Updates product data.

        Parameters
        ----------
        query : ast.Update
            SQL UPDATE query

        Returns
        -------
        int
            The number of rows updated
        """
        values = query.values
        conditions = query.where
        stripe.api_key = self.handler.connection_data['api_key']

        try:
            product_id = conditions['id']  # Assuming 'id' is the condition for update
            updated_product = stripe.Product.modify(
                product_id,
                name=values['name'],
                description=values['description'],
            )
            return 1  
        except stripe.error.StripeError as e:
            log.logger.error(f"Stripe Error: {e}")
            return 0

    def delete(self, query: ast.Delete) -> int:
        """
        Deletes product data.

        Parameters
        ----------
        query : ast.Delete
            SQL DELETE query

        Returns
        -------
        int
            The number of rows deleted
        """
        conditions = query.where
        stripe.api_key = self.handler.connection_data['api_key']

        try:
            product_id = conditions['id']  # Assuming 'id' is the condition for delete
            deleted_product = stripe.Product.delete(product_id)
            return 1 
        except stripe.error.StripeError as e:
            log.logger.error(f"Stripe Error: {e}")
            return 0  
