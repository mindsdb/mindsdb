import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class AccountsTable(APITable):

    def flatten_dict(self, data: dict, prefix: str = ""):
        flat_data = {}
        for key, value in data.items():
            if isinstance(value, dict):
                flattened_sub_dict = self.flatten_dict(value, f"{key}_")
                flat_data.update(flattened_sub_dict)
            else:
                flat_data[f"{prefix}{key}"] = value
        return flat_data

    def select(self, query: ast.Select) -> pd.DataFrame:
        qbo = self.handler.connect()
        accounts_data = qbo.accounts.get()
        flattened_accounts_data = [self.flatten_dict(account) for account in accounts_data]
        result = pd.DataFrame(flattened_accounts_data)
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
            'CurrencyRef_value',
            'CurrencyRef_name',
            'domain',
            'sparse',
            'Id',
            'SyncToken',
            'MetaData_CreateTime',
            'MetaData_LastUpdatedTime',
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
