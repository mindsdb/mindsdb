from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser import ast

import pandas as pd


class CoinBaseAggregatedTradesTable(APITable):

    DEFAULT_INTERVAL = 60
    DEFAULT_SYMBOL = 'BTC-USD'

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the CoinBase API and returns it as a pandas DataFrame.

        Returns dataframe representing the CoinBase API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        conditions = extract_comparison_conditions(query.where)

        params = {
            'interval': CoinBaseAggregatedTradesTable.DEFAULT_INTERVAL,
            'symbol': CoinBaseAggregatedTradesTable.DEFAULT_SYMBOL,
        }
        for op, arg1, arg2 in conditions:
            if arg1 == 'interval':
                if op != '=':
                    raise NotImplementedError
                params['interval'] = arg2

            elif arg1 == 'symbol':
                if op != '=':
                    raise NotImplementedError
                params['symbol'] = arg2

        coinbase_candle_data = self.handler.call_coinbase_api(
            method_name='get_candle',
            params=params
        )

        return coinbase_candle_data

    def get_columns(self):
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'symbol',
            'low',
            'high',
            'open',
            'close',
            'volume',
            'timestamp',
            'current_time'
        ]
