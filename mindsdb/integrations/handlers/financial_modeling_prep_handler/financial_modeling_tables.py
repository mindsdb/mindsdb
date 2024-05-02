ffrom mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.utilities.date_utils import interval_str_to_duration_ms, utc_date_str_to_timestamp_ms
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import pandas as pd
import time


class FinancialModelingTradesTable(APITable):
    def _get_daily_endpoint_params_from_conditions(self, conditions: List) -> Dict:
        params = {}
        for op, arg1, arg2 in conditions: 
            if arg1 == 'symbol':
                if op != '=':
                    raise NotImplementedError
                params['symbol'] = arg2
            if arg1 == "from":
                if op != '=':
                    raise NotImplementedError
                params['from'] = arg2
            if arg1 == "to":
                if op != '=':
                    raise NotImplementedError
                params['to'] = arg2

        return params

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the FinancialModeling API and returns it as a pandas DataFrame.
        
        Returns dataframe representing the FinancialModeling API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        conditions = extract_comparison_conditions(query.where)
        params = self._get_daily_endpoint_params_from_conditions(conditions)

        daily_chart_table = self.handler.call_financial_modeling_api(
                method_name='daily_chart',
                params=params
        )
        
        return daily_chart_table

    def get_columns(self):
        return [
            'symbol',
            'from',
            'to',
            'close_price'
        ]

    