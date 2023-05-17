import stripe
import pandas as pd
from typing import Text, List, Dict

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable


class CustomersTable(APITable):
    """The Stripe Customers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Stripe "GET /customers" API endpoint.

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

        pass

    def get_columns(self) -> List[Text]:
        pass

    def get_customers(self, **kwargs) -> List[Dict]:
        stripe = self.handler.connect()
