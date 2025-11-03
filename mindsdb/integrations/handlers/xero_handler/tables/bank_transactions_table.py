from typing import List
import pandas as pd
from mindsdb_sql_parser import ast
from mindsdb.integrations.handlers.xero_handler.xero_tables import XeroTable, extract_comparison_conditions
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser
from xero_python.accounting import AccountingApi


class BankTransactionsTable(XeroTable):
    """Table for Xero Bank Transactions"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "date": {"type": "where", "xero_field": "Date", "value_type": "date"},
        "contact_id": {"type": "where", "xero_field": "Contact.ContactID", "value_type": "string"},
    }

    COLUMN_REMAP = {
        "contact_contact_id": "contact_id",
        "contact_contact_number": "contact_number",
        "contact_contact_persons": "contact_persons",
        "contact_contact_groups": "contact_groups",
        "bank_account_bank_account_number": "bank_account_number",
        "bank_account_bank_account_type": "bank_account_type",
    }

    def get_columns(self) -> List[str]:
        return [
            "bank_account__class",
            "bank_account_account_id",
            "bank_account_add_to_watchlist",
            "bank_account_code",
            "bank_account_currency_code",
            "bank_account_description",
            "bank_account_enable_payments_to_account",
            "bank_account_has_attachments",
            "bank_account_name",
            "bank_account_number",
            "bank_account_reporting_code",
            "bank_account_reporting_code_name",
            "bank_account_show_in_expense_claims",
            "bank_account_status",
            "bank_account_system_account",
            "bank_account_tax_type",
            "bank_account_type",
            "bank_account_type",
            "bank_account_updated_date_utc",
            "bank_account_validation_errors",
            "bank_transaction_id",
            "contact_account_number",
            "contact_accounts_payable_tax_type",
            "contact_accounts_receivable_tax_type",
            "contact_addresses",
            "contact_attachments",
            "contact_balances",
            "contact_bank_account_details",
            "contact_batch_payments",
            "contact_branding_theme",
            "contact_company_number",
            "contact_default_currency",
            "contact_discount",
            "contact_email_address",
            "contact_first_name",
            "contact_groups",
            "contact_has_attachments",
            "contact_has_validation_errors",
            "contact_id",
            "contact_is_customer",
            "contact_is_supplier",
            "contact_last_name",
            "contact_merged_to_contact_id",
            "contact_name",
            "contact_number",
            "contact_payment_terms",
            "contact_persons",
            "contact_phones",
            "contact_purchases_default_account_code",
            "contact_purchases_default_line_amount_type",
            "contact_purchases_tracking_categories",
            "contact_sales_default_account_code",
            "contact_sales_default_line_amount_type",
            "contact_sales_tracking_categories",
            "contact_status",
            "contact_status_attribute_string",
            "contact_tax_number",
            "contact_tracking_category_name",
            "contact_tracking_category_option",
            "contact_updated_date_utc",
            "contact_validation_errors",
            "contact_website",
            "contact_xero_network_key",
            "currency_code",
            "currency_rate",
            "date",
            "has_attachments",
            "is_reconciled",
            "line_amount_types",
            "line_items",
            "overpayment_id",
            "prepayment_id",
            "reference",
            "status",
            "status_attribute_string",
            "sub_total",
            "total",
            "total_tax",
            "type",
            "updated_date_utc",
            "url",
            "validation_errors"
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
            # Fetch bank transactions with optimized parameters
            bank_transactions = api.get_bank_transactions(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(bank_transactions.bank_transactions or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch bank transactions: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            from mindsdb.integrations.utilities.sql_utils import filter_dataframe
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "bank_transactions", columns=self.get_columns()
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