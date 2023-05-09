import shopify
import pandas as pd
from typing import Text, List, Dict

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from utils import parse_statement, get_results


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

        selected_columns, where_conditions, order_by_conditions, result_limit = parse_statement(
            query,
            'products',
            self.get_columns()
        )

        products_df = pd.json_normalize(self.get_products(limit=result_limit))

        products_df = get_results(products_df, selected_columns, where_conditions, order_by_conditions)

        return products_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_products(limit=1)).columns.tolist()

    def get_products(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        products = shopify.Product.find(**kwargs)
        return [product.to_dict() for product in products]


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
        selected_columns, where_conditions, order_by_conditions, total_results = parse_statement(
            query,
            'customers',
            self.get_columns()
        )

        customers_df = pd.json_normalize(self.get_customers(limit=total_results))

        customers_df = get_results(customers_df, selected_columns, where_conditions, order_by_conditions)

        return customers_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_customers(limit=1)).columns.tolist()

    def get_customers(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        customers = shopify.Customer.find(**kwargs)
        return [customer.to_dict() for customer in customers]


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
        selected_columns, where_conditions, order_by_conditions, total_results = parse_statement(
            query,
            'orders',
            self.get_columns()
        )

        orders_df = pd.json_normalize(self.get_orders(limit=total_results))

        orders_df = get_results(orders_df, selected_columns, where_conditions, order_by_conditions)

        return orders_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_orders(limit=1)).columns.tolist()

    def get_orders(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        orders = shopify.Order.find(**kwargs)
        return [order.to_dict() for order in orders]