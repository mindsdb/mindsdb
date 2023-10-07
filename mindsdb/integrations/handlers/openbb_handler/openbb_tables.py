from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.utilities.date_utils import interval_str_to_duration_ms, utc_date_str_to_timestamp_ms
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import pandas as pd
import time


class OpenBBtable(APITable):

    def _get_params_from_conditions(self, conditions: List) -> Dict:
        """Gets aggregate trade data API params from SQL WHERE conditions.
        
        Returns params to use for Binance API call to klines.

        Args:
            conditions (List): List of individual SQL WHERE conditions.
        """
        params: dict = {}
        # generic interpreter for conditions
        # since these are all equality conditions due to OpenBB Platform's API
        # then we can just use the first arg as the key and the second as the value
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError
            params[arg1] = arg2

        return params

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the OpenBB Platform and returns it as a pandas DataFrame.
        
        Returns dataframe representing the OpenBB data.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        conditions = extract_comparison_conditions(query.where)
        params = self._get_params_from_conditions(conditions)

        openbb_data = self.handler.call_openbb_api(
            method_name='stocks_load',
            params=params,
        )

        return openbb_data
