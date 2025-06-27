import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast


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


class VendorsTable(APITable):

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
        vendors_data = qbo.vendors.get()
        flattened_vendors_data = [self.flatten_dict(vendor) for vendor in vendors_data]
        result = pd.DataFrame(flattened_vendors_data)
        self.filter_columns(result, query)
        return result

    def get_columns(self):
        return [
            'Balance',
            'Vendor1099',
            'CurrencyRef_value',
            'CurrencyRef_name',
            'Id',
            'SyncToken',
            'MetaData_CreateTime',
            'MetaData_LastUpdatedTime',
            'DisplayName',
            'PrintOnCheckName',
            'Active',
            'domain',
            'sparse'
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


class BillsTable(APITable):

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
        bills_data = qbo.bills.get()
        flattened_bills_data = [self.flatten_dict(bill) for bill in bills_data]
        result = pd.DataFrame(flattened_bills_data)
        self.filter_columns(result, query)
        return result

    def get_columns(self):
        return [
            'DueDate',
            'Balance',
            'Id',
            'SyncToken',
            'MetaData_CreateTime',
            'MetaData_LastUpdatedTime',
            'TxnDate',
            'CurrencyRef_value',
            'CurrencyRef_name',
            'VendorRef_value',
            'VendorRef_name',
            'APAccountRef_value',
            'APAccountRef_name',
            'TotalAmt',
            'Line_0_Id',
            'Line_0_LineNum',
            'Line_0_Description',
            'Line_0_Amount',
            'Line_0_DetailType',
            'Line_0_ItemBasedExpenseLineDetail_BillableStatus',
            'Line_0_ItemBasedExpenseLineDetail_ItemRef_value',
            'Line_0_ItemBasedExpenseLineDetail_ItemRef_name',
            'Line_0_ItemBasedExpenseLineDetail_UnitPrice',
            'Line_0_ItemBasedExpenseLineDetail_Qty',
            'Line_1_Id',
            'Line_1_LineNum',
            'Line_1_Description',
            'Line_1_Amount',
            'Line_1_DetailType',
            'Line_1_ItemBasedExpenseLineDetail_BillableStatus',
            'Line_1_ItemBasedExpenseLineDetail_ItemRef_value',
            'Line_1_ItemBasedExpenseLineDetail_ItemRef_name',
            'Line_1_ItemBasedExpenseLineDetail_UnitPrice',
            'Line_1_ItemBasedExpenseLineDetail_Qty',
            'domain',
            'sparse'
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


class EmployeesTable(APITable):

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
        employees_data = qbo.employees.get()
        flattened_employees_data = [self.flatten_dict(employee) for employee in employees_data]
        result = pd.DataFrame(flattened_employees_data)
        self.filter_columns(result, query)
        return result

    def get_columns(self):
        return [
            'BillableTime',
            'Id',
            'SyncToken',
            'MetaData_CreateTime',
            'MetaData_LastUpdatedTime',
            'GivenName',
            'FamilyName',
            'DisplayName',
            'PrintOnCheckName',
            'Active',
            'PrimaryPhone_FreeFormNumber',
            'HiredDate',
            'domain',
            'sparse'
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
