import shopify
import pandas as pd

from typing import Text, List, Dict

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb_sql.parser import ast

from mindsdb.utilities import log


def parse_statement(query: ast.Select, table: Text, columns: List[Text]):
    # SELECT
    selected_columns = []
    for target in query.targets:
        if isinstance(target, ast.Star):
            selected_columns = columns
            break
        elif isinstance(target, ast.Identifier):
            selected_columns.append(target.parts[-1])
        else:
            raise ValueError(f"Unknown query target {type(target)}")

    # WHERE
    where_conditions = extract_comparison_conditions(query.where)

    # ORDER BY
    order_by_conditions = {}
    if query.order_by and len(query.order_by) > 0:
        order_by_conditions["columns"] = []
        order_by_conditions["ascending"] = []

        for an_order in query.order_by:
            if an_order.field.parts[0] == table:
                if an_order.field.parts[1] in columns:
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

    # LIMIT
    if query.limit:
        result_limit = query.limit.value
    else:
        result_limit = 20

    return selected_columns, where_conditions, order_by_conditions, result_limit


def get_results(df, selected_columns, where_conditions, order_by_conditions, result_limit=None):
    if result_limit:
        df = df.head(result_limit)

    if len(where_conditions) > 0:
        df = filter_df(df, where_conditions)

    if len(df) == 0:
        df = pd.DataFrame([], columns=selected_columns)
    else:
        df = df[selected_columns]

        if len(order_by_conditions.get("columns", [])) > 0:
            df = df.sort_values(
                by=order_by_conditions["columns"],
                ascending=order_by_conditions["ascending"],
            )

    return df


def filter_df(df, conditions):
    for condition in conditions:
        column = condition[1]
        operator = '==' if condition[0] == '=' else condition[0]
        value = f"'{condition[2]}'" if type(condition[2]) == str else condition[2]

        query = f"{column} {operator} {value}"
        df.query(query, inplace=True)

    return df


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

        selected_columns, where_conditions, order_by_conditions, total_results = parse_statement(
            query,
            'products',
            self.get_columns()
        )

        products_df = pd.json_normalize(self.get_products(limit=total_results))

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
        products = shopify.Customer.find(**kwargs)
        return [product.to_dict() for product in products]