from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.utilities.date_utils import interval_str_to_duration_ms, utc_date_str_to_timestamp_ms
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import pandas as pd
import time



def create_table_class(params_metadata, response_metadata, obb_function):

    mandatory_fields = params_metadata['required'] if 'required' in params_metadata else []
    response_columns = list(response_metadata['properties'].keys())

    class AnyTable(APITable):
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
                if op == "=":
                    params[arg1] = arg2

            return params

        def select(self, query: ast.Select) -> pd.DataFrame:
            """Selects data from the OpenBB Platform and returns it as a pandas DataFrame.

            Returns dataframe representing the OpenBB data.

            Args:
                query (ast.Select): Given SQL SELECT query
            """
            conditions = extract_comparison_conditions(query.where)
            arg_params = self._get_params_from_conditions(conditions=conditions)
            params = {}
            filters = []
            mandatory_args = {key: False for key in mandatory_fields}
            columns_to_add = {}
            # filter only for properties in the
            strict_filter = arg_params.get('strict_filter', False)
            for op, arg1, arg2 in conditions:

                if op == 'or':
                    raise NotImplementedError('OR is not supported')
                
                if arg1 in mandatory_fields:
                    mandatory_args[arg1] = True


                if ('start_'+arg1  in params_metadata['properties'] 
                    and  arg1 in response_columns 
                    and arg2 is not None
                    and "format" in response_metadata['properties'][arg1]
                    ):
                    if response_metadata['properties'][arg1]["format"] != 'date-time':
                        
                        date = parse_local_date(arg2)
                        interval = arg_params.get('interval', '1d')
                        if op == '>':
                            params['start_'+arg1] = date.strftime('%Y-%m-%d')
                        elif op == '<':
                            params['end_'+arg1] = date.strftime('%Y-%m-%d')
                        elif op == '>=':
                            date = date - pd.Timedelta(interval)
                            params['start_'+arg1] = date.strftime('%Y-%m-%d')
                        elif op == '<=':
                            date = date + pd.Timedelta(interval)
                            params['end_'+arg1] = date.strftime('%Y-%m-%d')
                        elif op == '=':
                            date = date - pd.Timedelta(interval)
                            params['start_'+arg1] = date.strftime('%Y-%m-%d')
                            date = date + pd.Timedelta(interval)
                            params['end_'+arg1] = date.strftime('%Y-%m-%d')
                
                elif arg1 in params_metadata['properties'] or not strict_filter:
                    if op == '=':
                        params[arg1] = arg2
                        columns_to_add[arg1] = arg2
                
                
                filters.append([op, arg1, arg2])

            if not all(mandatory_args.values()):
                string = 'You must specify the following arguments in the WHERE statement:'
                for key in mandatory_args:
                    if not mandatory_args[key]:
                        string += "\n--(required)---\n* {key}:\n{help}\n ".format(key=key, help=dict_to_yaml(params_metadata['properties'][key]))
                for key in params_metadata["properties"]:
                    if key not in mandatory_args:
                        string += "\n--(optional)---\n* {key}:\n{help}\n ".format(key=key, help=dict_to_yaml(params_metadata['properties'][key]))
                raise NotImplementedError(string)

            result = obb_function(**params).to_df()

            # Check if index is a datetime, if it is we want that as a column
            if isinstance(result.index, pd.DatetimeIndex):
                result.reset_index(inplace=True)
            

            if query.limit is not None:
                result = result.head(query.limit.value)

            for key in columns_to_add:
                result[key] = params[key]
            
            # filter targets
            result = filter_dataframe(result, filters)

            columns = self.get_columns()

            columns += [col for col in result.columns if col not in columns]

            # project targets
            result = project_dataframe(result, query.targets, columns)
            # test this
            if query.order_by:
                result = sort_dataframe(result, query.order_by)
            
            #result = group_by_df(resut, query)



            return result

        def get_columns(self):
            
            return response_columns
        
    return AnyTable


class BinanceAggregatedTradesTable(APITable):

    # Default 1m intervals in aggregate data.
    DEFAULT_AGGREGATE_TRADE_INTERVAL = '1m'
    DEFAULT_AGGREGATE_TRADE_LIMIT = 1000
    # Binance Spot client has connection pool size of 10.
    MAX_THREAD_POOL_WORKERS = 10

    def _get_batch_klines(self, executor: ThreadPoolExecutor, total_results: int, params: Dict) -> pd.DataFrame:
        """Gets aggregate trade data in batches and combines the results together.
        
        Returns all results as a pandas DataFrame.

        Args:
            executor (ThreadPoolExecutor): Executor to use when mapping API calls as tasks.
            total_results (int): Total number of results to fetch.
            params (Dict): Overall request params to be split into batches.
        """
        interval_duration_ms = interval_str_to_duration_ms(params['interval'])
        if 'end_time' not in params:
            # Default to get all klines before the current time.
            overall_end_ms = int(time.time() * 1000)
        else:
            overall_end_ms = params['end_time']

        if 'start_time' not in params:
            total_duration_ms = interval_duration_ms * total_results
            # Infer start time based on the interval and how many klines we need to fetch.
            overall_start_ms = overall_end_ms - total_duration_ms
        else:
            overall_start_ms = params['start_time']

        next_params = params.copy()
        next_params['start_time'] = overall_start_ms
        duration_per_api_call_ms = interval_duration_ms * BinanceAggregatedTradesTable.DEFAULT_AGGREGATE_TRADE_LIMIT
        next_params['end_time'] = min(overall_end_ms, overall_start_ms + duration_per_api_call_ms)
        all_params = [next_params.copy()]
        results_so_far = BinanceAggregatedTradesTable.DEFAULT_AGGREGATE_TRADE_LIMIT
        while next_params['end_time'] < overall_end_ms and results_so_far < total_results:
            next_params['limit'] = min(
                BinanceAggregatedTradesTable.DEFAULT_AGGREGATE_TRADE_LIMIT,
                total_results - results_so_far
            )
            next_params['start_time'] = next_params['end_time']
            next_params['end_time'] = min(overall_end_ms, next_params['start_time'] + duration_per_api_call_ms)
            all_params.append(next_params.copy())
            results_so_far += next_params['limit']

        aggregated_trade_subdatas = list(executor.map(lambda p: self.handler.call_binance_api(method_name='klines', params=p), all_params))
        if not aggregated_trade_subdatas:
            return pd.DataFrame([])

        aggregated_trade_data = aggregated_trade_subdatas[0]
        for aggregated_trade_subdata in aggregated_trade_subdatas[1:]:
            aggregated_trade_data = pd.concat([aggregated_trade_data, aggregated_trade_subdata])
        return aggregated_trade_data

    def _get_kline_params_from_conditions(self, conditions: List) -> Dict:
        """Gets aggregate trade data API params from SQL WHERE conditions.
        
        Returns params to use for Binance API call to klines.

        Args:
            conditions (List): List of individual SQL WHERE conditions.
        """
        params = {
            'interval': BinanceAggregatedTradesTable.DEFAULT_AGGREGATE_TRADE_INTERVAL,
            'limit': BinanceAggregatedTradesTable.DEFAULT_AGGREGATE_TRADE_LIMIT
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
        interval_duration_ms = interval_str_to_duration_ms(params['interval'])

        for op, arg1, arg2 in conditions:
            if arg1 == 'open_time':
                utc_timestamp_ms = utc_date_str_to_timestamp_ms(arg2)
                if op == '>':
                    params['start_time'] = utc_timestamp_ms
                else:
                    raise NotImplementedError
                continue
            elif arg1 == 'close_time':
                utc_timestamp_ms = utc_date_str_to_timestamp_ms(arg2)
                if op == '<':
                    params['end_time'] = utc_timestamp_ms - interval_duration_ms
                else:
                    raise NotImplementedError

        return params

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the Binance API and returns it as a pandas DataFrame.
        
        Returns dataframe representing the Binance API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        conditions = extract_comparison_conditions(query.where)
        params = self._get_kline_params_from_conditions(conditions)

        total_results = params['limit']
        if query.limit:
            total_results = query.limit.value
            params['limit'] = min(BinanceAggregatedTradesTable.DEFAULT_AGGREGATE_TRADE_LIMIT, query.limit.value)

        if total_results > BinanceAggregatedTradesTable.DEFAULT_AGGREGATE_TRADE_LIMIT:
            # Max 1000 klines per API call so we need to combine multiple API calls.
            with ThreadPoolExecutor(max_workers=BinanceAggregatedTradesTable.MAX_THREAD_POOL_WORKERS) as executor:
                aggregated_trades_data = self._get_batch_klines(executor, total_results, params)

        else:
            aggregated_trades_data = self.handler.call_binance_api(
                method_name='klines',
                params=params
            )

        # Only return the columns we need to.
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(aggregated_trades_data) == 0:
            aggregated_trades_data = pd.DataFrame([], columns=columns)
        else:
            # Remove columns not part of select.
            aggregated_trades_data.columns = self.get_columns()
            for col in set(aggregated_trades_data.columns).difference(set(columns)):
                aggregated_trades_data = aggregated_trades_data.drop(col, axis=1)

        return aggregated_trades_data

    def get_columns(self):
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'symbol',
            'open_time',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'volume',
            'close_time',
            'quote_asset_volume',
            'number_of_trades',
            'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume'
        ]
