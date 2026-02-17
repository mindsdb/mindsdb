from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser import ast

from typing import Dict, List

import pandas as pd

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
import requests


class HistoricalPriceTable(APITable):

    def _get_historical_price_endpoint_params_from_conditions(self, conditions: List) -> Dict:
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'symbol':
                if op != '=':
                    raise NotImplementedError
                params['symbol'] = arg2
            if arg1 == "from_date":
                if op != '=':
                    raise NotImplementedError
                params['from'] = arg2
            if arg1 == "to_date":
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
        params = self._get_historical_price_endpoint_params_from_conditions(conditions)

        if query.limit and query.limit.value:
            limit_value = query.limit.value
            params['limit'] = limit_value

        historical_prices = self.get_historical_price_chart(params=params)

        return historical_prices

    def get_historical_price_chart(self, params: Dict = None) -> pd.DataFrame:
        base_url = self.handler.connect()
        if 'symbol' not in params:
            raise ValueError('Missing "symbol" param')
        symbol = params['symbol']
        params.pop('symbol')

        limitParam = False
        limit = 0
        if 'limit' in params:
            limit = params['limit']
            params.pop('limit')
            limitParam = True

        url = f"{base_url}{symbol}"  # https://financialmodelingprep.com/api/v3/historical-price-full/<symbol>
        param = {'apikey': self.handler.api_key, **params}

        response = requests.get(url, param)
        historical_data = response.json()
        historical = historical_data.get("historical")

        if limitParam:
            return pd.DataFrame(historical).head(limit)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                historical
            )
        )

        if historical:
            return pd.DataFrame(historical)
        else:
            return pd.DataFrame()
