import paypalrestsdk
import pandas as pd
from typing import Text, List, Dict

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor


class PaymentsTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
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

        select_statement_parser = SELECTQueryParser(
            query,
            'payments',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        payments_df = pd.json_normalize(self.get_payments(count=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            payments_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        payments_df = select_statement_executor.execute_query()

        return payments_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_payments(count=1)).columns.tolist()

    def get_payments(self, **kwargs) -> List[Dict]:
        connection = self.handler.connect()
        payments = paypalrestsdk.Payment.all(kwargs, api=connection)
        return [payment.to_dict() for payment in payments['payments']]
