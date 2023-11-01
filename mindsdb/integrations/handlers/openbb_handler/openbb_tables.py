from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.date_utils import parse_local_date
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, project_dataframe, filter_dataframe
from mindsdb.integrations.utilities.sql_utils import sort_dataframe
from mindsdb.integrations.utilities.utils import dict_to_yaml
from typing import Dict, List

import pandas as pd


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

                if ('start_' + arg1 in params_metadata['properties']
                    and arg1 in response_columns and arg2 is not None
                        and "format" in response_metadata['properties'][arg1]):
                    if response_metadata['properties'][arg1]["format"] != 'date-time':
                        date = parse_local_date(arg2)
                        interval = arg_params.get('interval', '1d')
                        if op == '>':
                            params['start_' + arg1] = date.strftime('%Y-%m-%d')
                        elif op == '<':
                            params['end_' + arg1] = date.strftime('%Y-%m-%d')
                        elif op == '>=':
                            date = date - pd.Timedelta(interval)
                            params['start_' + arg1] = date.strftime('%Y-%m-%d')
                        elif op == '<=':
                            date = date + pd.Timedelta(interval)
                            params['end_' + arg1] = date.strftime('%Y-%m-%d')
                        elif op == '=':
                            date = date - pd.Timedelta(interval)
                            params['start_' + arg1] = date.strftime('%Y-%m-%d')
                            date = date + pd.Timedelta(interval)
                            params['end_' + arg1] = date.strftime('%Y-%m-%d')
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

            return result

        def get_columns(self):
            return response_columns
    return AnyTable
