from typing import List
import pandas as pd
from mindsdb_sql_parser import ast
from mindsdb.integrations.handlers.xero_handler.xero_tables import (
    XeroTable, 
    extract_comparison_conditions,
    filter_dataframe,
    sort_dataframe
)
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser
from xero_python.accounting import AccountingApi

class ContactsTable(XeroTable):
    """Table for Xero Contacts"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "id": {"type": "id_list", "param": "ids"},
        "name": {"type": "where", "param": "SearchTerm", "value_type": "string"},
        "first_name": {"type": "where", "param": "SearchTerm", "value_type": "string"},
        "last_name": {"type": "where", "param": "SearchTerm", "value_type": "string"},
        "email_address": {"type": "param", "param": "SearchTerm", "value_type": "string"},
        "contact_number": {"type": "where", "param": "SearchTerm", "value_type": "string"},
        "company_number": {"type": "where", "param": "SearchTerm", "value_type": "string"},
        "status": {"type": "where", "param": "status", "value_type": "string"},
    }
    
    COLUMN_REMAP = {}

    def get_columns(self) -> List[str]:
        return [
            "contact_id",
            "contact_status",
            "name",
            "first_name",
            "last_name",
            "company_number",
            "email_address",
            "bank_account_details",
            "tax_number",
            "accounts_receivable_tax_type",
            "accounts_payable_tax_type",
            "addresses",
            "phones",
            "updated_date_utc",
            "is_supplier",
            "is_customer",
            "default_currency"
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
            # Fetch contacts with optimized parameters
            contacts = api.get_contacts(xero_tenant_id=self.handler.tenant_id, summary_only=False, **api_params)
            df = self._convert_response_to_dataframe(contacts.contacts or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch contacts: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "contacts", columns=self.get_columns()
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
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df