import stripe
import pandas as pd
from typing import Text, List, Dict

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from .utils import parse_statement, get_results


class CustomersTable(APITable):
    """The Stripe Customers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls data from the Stripe Customers.

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

        selected_columns, where_conditions, order_by_conditions, result_limit = parse_statement(
            query,
            'products',
            self.get_columns()
        )

        customers_df = pd.json_normalize(self.get_customers(limit=result_limit))

        customers_df = get_results(customers_df, selected_columns, where_conditions, order_by_conditions)

        return customers_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_customers(limit=1)).columns.tolist()

    def get_customers(self, **kwargs) -> List[Dict]:
        stripe = self.handler.connect()
        customers = stripe.Customer.list(**kwargs)
        return [customer.to_dict() for customer in customers]
