import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class AccountsTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        qbo = self.handler.connect()
        accounts_data = qbo.accounts.get()
        result = pd.DataFrame(accounts_data)
        self.filter_columns(result, query)
        return result

    def get_columns(self):
        return [
            'Name',
            'SubAccount',
            'FullyQualifiedName',
            'Active',
            'Classification',
            'AccountType',
            'AccountSubType',
            'CurrentBalance',
            'CurrentBalanceWithSubAccounts',
            'CurrencyRef',
            'domain',
            'sparse',
            'Id',
            'SyncToken',
            'MetaData',
        ]

    def filter_columns(self, result: pd.DataFrame, query: ast.Select = None):
        columns = []
        if query is not None:
            for target in query.targets:
                if isinstance(target, ast.Star):
                    columns = self.get_columns()
                    break
                elif isinstance(target, ast.Identifier):
                    columns.append(target.value)
        if len(columns) > 0:
            result = result[columns]
