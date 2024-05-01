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
        for op, arg1, arg2, arg3, arg4, arg5, arg6 in conditions:
            if arg1 == 'symbol':
                if op != '=':
                    raise NotImplementedError
                params['symbol'] = arg2
            if arg3 == "from":
                if op != '=':
                    raise NotImplementedError
                params['from'] = arg4
            if arg5 == "to":
                if op != '=':
                    raise NotImplementedError
                params['to'] = arg6

        return params

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the FinancialModeling API and returns it as a pandas DataFrame.
        
        Returns dataframe representing the FinancialModeling API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        conditions = extract_comparison_conditions(query.where)
        params = self._get_daily_endpoint_params_from_conditions(conditions)




    