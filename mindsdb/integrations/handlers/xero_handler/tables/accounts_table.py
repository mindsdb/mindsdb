from typing import List
import pandas as pd
from mindsdb_sql_parser import ast
from mindsdb.integrations.handlers.xero_handler.xero_tables import XeroTable, extract_comparison_conditions
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser
from xero_python.accounting import AccountingApi


class AccountsTable(XeroTable):
    """Table for Xero Chart of Accounts"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "code": {"type": "where", "xero_field": "Code", "value_type": "string"},
        "name": {"type": "where", "xero_field": "Name", "value_type": "string"},
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "currency_code": {"type": "where", "xero_field": "CurrencyCode", "value_type": "string"},
        "account_class": {"type": "where", "xero_field": "Class", "value_type": "string"},
        "system_account": {"type": "where", "xero_field": "SystemAccount", "value_type": "string"},
        "tax_type": {"type": "where", "xero_field": "TaxType", "value_type": "string"},
    }

    def get_columns(self) -> List[str]:
        return [
            "account_id",
            "code",
            "name",
            "type",
            "account_class",
            "tax_type",
            "description",
            "currency_code",
            "bank_account_number",
            "bank_account_type",
            "enable_payments_to_account",
            "show_in_expense_claims",
            "system_account",
            "reporting_code",
            "reporting_code_name",
            "status",
            "updated_utc",
            "has_attachments",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch accounts from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Extract and parse WHERE conditions
        api_params = {}
        remaining_conditions = []

        if query.where:
            conditions = extract_comparison_conditions(query.where)
            api_params, remaining_conditions = self._parse_conditions_for_api(
                conditions, self.SUPPORTED_FILTERS
            )

        try:
            # Fetch accounts with optimized parameters
            accounts = api.get_accounts(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(accounts.accounts or [])
        except Exception as e:
            raise Exception(f"Failed to fetch accounts: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            from mindsdb.integrations.utilities.sql_utils import filter_dataframe
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "accounts", columns=self.get_columns()
        )
        selected_columns, _, order_by_conditions, result_limit = parser.parse_query()

        # Apply column selection
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)
        else:
            available_columns = [col for col in selected_columns if col in df.columns]
            df = df[available_columns]

        # Apply ordering
        if order_by_conditions:
            from mindsdb.integrations.utilities.sql_utils import sort_dataframe
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df