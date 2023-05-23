import pandas as pd
from typing import Text, List, Dict

from mindsdb_sql import ASTNode
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from .utils import parse_statement, get_results


class PaymentsTable(APITable):

    def select(self, query: ASTNode) -> pd.DataFrame:
        """
        Pulls PayPal Payments data.
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            PayPal Payments matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        pass

    def get_columns(self) -> List[Text]:
        pass

    def get_payments(self, **kwargs) -> List[Dict]:
        pass
