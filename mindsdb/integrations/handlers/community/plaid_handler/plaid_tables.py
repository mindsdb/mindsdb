import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser import ast


class BalanceTable(APITable):
    '''A class representing the balance table.

    This class inherits from APITable and provides functionality to select data
    from the balance endpoint of the Plaid API and return it as a pandas DataFrame.

    Methods:
        select(ast.Select): Select data from the balance table and return it as a pandas DataFrame.
        get_columns(): Get the list of column names for the balance table.

    '''

    def select(self, query: ast.Select):
        '''Select data from the balance table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        '''

        conditions = extract_comparison_conditions(query.where)
        params = {}
        for i in conditions:
            if i[1] == 'last_updated_datetime':
                if i[0] == '=':
                    params[i[1]] = i[2]
                else:
                    raise Exception("Only equals to '=' is Supported with 'last_updated_datetime'")

        result = self.handler.call_plaid_api(method_name='get_balance', params=params)

        self.filter_columns(query=query, result=result)
        return result

    def get_columns(self):
        '''Get the list of column names for the balance table.

        Returns:
            list: A list of column names for the balance table.

        '''
        return [
            'account_id',
            'account_name',
            'account_mask',
            'account_type',
            'account_subtype',
            'account_official_name',
            'balance_iso_currency_code',
            'balance_unofficial_currency_code',
            'balance_available',
            'balance_current',
            'balance_limit'
        ]

    def filter_columns(self, result: pd.DataFrame, query: ast.Select = None):

        columns = []
        if query is not None:
            for target in query.targets:
                if isinstance(target, ast.Star):
                    columns = self.get_columns()
                    break
                elif isinstance(target, ast.Identifier):
                    columns.append(target.parts[-1])
                else:
                    raise NotImplementedError
        else:
            columns = self.get_columns()

        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None
            result = result[columns]

        if query is not None and query.limit is not None:
            return result.head(query.limit.value)

        return result


class TransactionTable(BalanceTable):
    '''A class representing the transaction table.

    This class inherits from APITable and provides functionality to select data
    from the transactions endpoint of the Plaid API and return it as a pandas DataFrame.

    Methods:
        select(ast.Select): Select data from the transaction table and return it as a pandas DataFrame.
        get_columns(): Get the list of column names for the transaction table.

    '''

    def select(self, query: ast.Select):
        '''Select data from the transaction table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        '''
        all_conditions = extract_comparison_conditions(query.where)
        condition = []
        params = {}
        for op, v, c in all_conditions:

            op = '==' if op == '=' else op  # converting '=' to '=='

            if (v == 'start_date' or v == 'end_date'):
                params[v] = c

            elif v in ['date', 'authorized_date'] or isinstance(c, str):
                condition.append(f"({v}{op}'{c}')")

            else:
                condition.append(f"({v}{op}{c})")

        merge_condition = ' and '.join(condition)

        result = self.handler.call_plaid_api(method_name='get_transactions', params=params)
        if merge_condition != '':
            result = result.query(merge_condition)

        result = self.filter_columns(query=query, result=result)
        return result

    def get_columns(self):
        '''Get the list of column names for the transaction table.

        Returns:
            list: A list of column names for the transaction table.
        '''
        return [
            'account_id',
            'transaction_id',
            'amount',
            'iso_currency_code',
            'check_number',
            'date',
            'authorized_date',
            'merchant_name',
            'payment_channel',
            'pending',
        ]
