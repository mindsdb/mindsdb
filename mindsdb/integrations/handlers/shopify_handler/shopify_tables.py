import shopify
import pandas as pd
from typing import Text, List, Dict, Any

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser
from mindsdb.integrations.handlers.utilities.query_utilities.delete_query_utilities import DELETEQueryParser, DELETEQueryExecutor
from mindsdb.integrations.handlers.utilities.query_utilities.update_query_utilities import UPDATEQueryParser, UPDATEQueryExecutor

from mindsdb.utilities.log import get_log

logger = get_log("integrations.shopify_handler")


class ProductsTable(APITable):
    """The Shopify Products Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /products" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Products matching the query

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
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /products" API endpoint.

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
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        products = shopify.Product.find(**kwargs)
        return [product.to_dict() for product in products]
    
    def delete_products(self, product_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for product_id in product_ids:
            product = shopify.Product.find(product_id)
            product.destroy()
            logger.info(f'Product {product_id} deleted')


class CustomersTable(APITable):
    """The Shopify Customers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /customers" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Customers matching the query

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

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /customers" API endpoint.

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
            supported_columns=['first_name', 'last_name', 'email', 'phone', 'tags', 'currency'],
            mandatory_columns=['first_name', 'last_name', 'email', 'phone'],
            all_mandatory=False
        )
        customer_data = insert_statement_parser.parse_query()
        self.create_customers(customer_data)

    def update(self, query: ast.Update) -> None:
        """Updates data in the Shopify "PUT /customers" API endpoint.

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

        customers_df = pd.json_normalize(self.get_customers())

        update_query_executor = UPDATEQueryExecutor(
            customers_df,
            where_conditions
        )

        customers_df = update_query_executor.execute_query()

        customer_ids = customers_df['id'].tolist()

        self.update_customers(customer_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_customers(limit=1)).columns.tolist()

    def get_customers(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        customers = shopify.Customer.find(**kwargs)
        return [customer.to_dict() for customer in customers]

    def create_customers(self, customer_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for customer in customer_data:
            created_customer = shopify.Customer.create(customer)
            if 'id' not in created_customer.to_dict():
                raise Exception('Customer creation failed')
            else:
                logger.info(f'Customer {created_customer.to_dict()["id"]} created')

    def update_customers(self, customer_ids: List[int], values_to_update: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for customer_id in customer_ids:
            customer = shopify.Customer.find(customer_id)
            for key, value in values_to_update.items():
                setattr(customer, key, value)
            customer.save()
            logger.info(f'Customer {customer_id} updated')


class OrdersTable(APITable):
    """The Shopify Orders Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /orders" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Orders matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'orders',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        orders_df = pd.json_normalize(self.get_orders(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            orders_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        orders_df = select_statement_executor.execute_query()

        return orders_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_orders(limit=1)).columns.tolist()

    def get_orders(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        orders = shopify.Order.find(**kwargs)
        return [order.to_dict() for order in orders]