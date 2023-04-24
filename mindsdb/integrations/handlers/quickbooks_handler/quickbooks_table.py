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


class PurchasesTable(APITable):

    def flatten_dict(self, data: dict, prefix: str = ""):
        flat_data = {}
        for key, value in data.items():
            if isinstance(value, dict):
                flattened_sub_dict = self.flatten_dict(value, f"{key}_")
                flat_data.update(flattened_sub_dict)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        flattened_sub_dict = self.flatten_dict(item, f"{key}_{i}_")
                        flat_data.update(flattened_sub_dict)
                    else:
                        flat_data[f"{prefix}{key}_{i}"] = item
            else:
                flat_data[f"{prefix}{key}"] = value
        return flat_data



    def select(self, query: ast.Select) -> pd.DataFrame:
        qbo = self.handler.connect()
        purchases_data = qbo.purchases.get()
        flattened_purchases_data = [self.flatten_dict(purchase) for purchase in purchases_data]
        result = pd.DataFrame(flattened_purchases_data)
        self.filter_columns(result, query)
        return result

    def get_columns(self):
        return [
            'AccountRef_value',
            'AccountRef_name',
            'PaymentType',
            'Credit',
            'TotalAmt',
            'domain',
            'sparse',
            'Id',
            'SyncToken',
            'MetaData_CreateTime',
            'MetaData_LastUpdatedTime',
            'TxnDate',
            'CurrencyRef_value',
            'CurrencyRef_name',
            'EntityRef_value',
            'EntityRef_name',
            'EntityRef_type',
            'Line_0_Id',
            'Line_0_Amount',
            'Line_0_DetailType'
            # Add more columns for additional line items if needed
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

class BillPaymentsTable(APITable):
    def flatten_dict(self, data: dict, prefix: str = ""):
        flat_data = {}
        for key, value in data.items():
            if isinstance(value, dict):
                flattened_sub_dict = self.flatten_dict(value, f"{prefix}{key}_")
                flat_data.update(flattened_sub_dict)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        flattened_sub_dict = self.flatten_dict(item, f"{prefix}{key}_{i}_")
                        flat_data.update(flattened_sub_dict)
                    else:
                        flat_data[f"{prefix}{key}_{i}"] = item
            else:
                flat_data[prefix + key] = value
        return flat_data

    def select(self, query: ast.Select) -> pd.DataFrame:
        qbo = self.handler.connect()
        billpayments_data = qbo.bill_payments.get()
        flattened_billpayments_data = [self.flatten_dict(bp) for bp in billpayments_data]
        result = pd.DataFrame(flattened_billpayments_data)
        self.filter_columns(result, query)
        return result

    def get_columns(self):
        return [
            'VendorRef_value',
            'VendorRef_name',
            'PayType',
            'CreditCardPayment_CCAccountRef_value',
            'CreditCardPayment_CCAccountRef_name',
            'CheckPayment_BankAccountRef_value',
            'CheckPayment_BankAccountRef_name',
            'TotalAmt',
            'Id',
            'SyncToken',
            'MetaData_CreateTime',
            'MetaData_LastUpdatedTime',
            'DocNumber',
            'TxnDate',
            'CurrencyRef_value',
            'CurrencyRef_name',
            'Line_0_Amount',
            'Line_0_LinkedTxn_0_TxnId',
            'Line_0_LinkedTxn_0_TxnType',
            'domain',
            'sparse',
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