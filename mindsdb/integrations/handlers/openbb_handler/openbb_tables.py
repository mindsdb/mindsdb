from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.date_utils import parse_local_date
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, project_dataframe, filter_dataframe
from mindsdb.integrations.utilities.sql_utils import sort_dataframe

from typing import Dict, List

import pandas as pd
import duckdb


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
            if op != "=":
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
            method_name="openbb_fetcher",
            params=params,
        )

        return openbb_data

class StocksLoadTable(APITable):
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
            if op != "=":
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
        
        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError('OR is not supported')
            if arg1 == 'date' and arg2 is not None:

                date = parse_local_date(arg2)

                if op == '>':
                    params['start_date'] = date
                elif op == '<':
                    params['end_date'] = date
                else:
                    raise NotImplementedError

                # also add to post query filter because date_sent_after=date1 will include date1
                filters.append([op, arg1, arg2])

            elif arg1 == 'symbol':
                if op == '=':
                    params['symbol'] = arg2
                # TODO: implement IN
                else:
                    NotImplementedError('Only  "symbol=" is implemented')

            elif arg1 == 'interval':
                if op == '=':
                    params['interval'] = arg2
               
                else:
                    NotImplementedError('Only  "interval=" is implemented')
            
            else:
                filters.append([op, arg1, arg2])

        
        result = self.handler.obb.stocks.load(**params).to_df()

        # Check if index is a datetime, if it is we want that as a column
        if isinstance(result.index, pd.DatetimeIndex):
            result.reset_index(inplace=True)

        result['symbol'] = params['symbol']
        
        # filter targets
        result = filter_dataframe(result, filters)

        # test this
        if query.order_by:
            result = sort_dataframe(result, query.order_by)

        # project targets
        columns = self.get_columns()
        columns += [col for col in result.columns if col not in columns]
        result = project_dataframe(result, query.targets, columns)

        if query.limit is not None:
            result = result.head(query.limit.value)

        #result = group_by_df(resut, query)


        # now we are going to try to runa  last filter on the query using duckdb magic
        # print
        # conn = duckdb.connect()
        # conn.register(table_name, result)
        # result = conn.execute(query_string).fetchdf()
        # conn.close()

        return result

    def get_columns(self):
        return [
            'date',
            'symbol', 
            'open', 
            'high', 
            'low', 
            'close', 
            'volume', 
            'vwap'
        ]