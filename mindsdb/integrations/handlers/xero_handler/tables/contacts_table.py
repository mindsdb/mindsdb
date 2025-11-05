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
        "contact_id": {"type": "id_list", "param": "i_ds"},
        "name": {"type": "direct", "param": "search_term", "value_type": "string"},
        "first_name": {"type": "direct", "param": "search_term", "value_type": "string"},
        "last_name": {"type": "direct", "param": "search_term", "value_type": "string"},
        "email_address": {"type": "direct", "param": "search_term", "value_type": "string"},
        "contact_number": {"type": "direct", "param": "search_term", "value_type": "string"},
        "company_number": {"type": "direct", "param": "search_term", "value_type": "string"},
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

        # Parse query to get result_limit (no default limit)
        parser = SELECTQueryParser(
            query, "contacts", columns=self.get_columns(), use_default_limit=False
        )
        selected_columns, _, order_by_conditions, result_limit = parser.parse_query()

        # Extract and parse WHERE conditions
        api_params = {}
        remaining_conditions = []

        if query.where:
            conditions = extract_comparison_conditions(query.where)
            api_params, remaining_conditions = self._parse_conditions_for_api(
                conditions, self.SUPPORTED_FILTERS
            )

        # Implement pagination to fetch all required records
        all_data = []
        page = 1
        page_size = 1000  # Xero's maximum page size
        records_fetched = 0

        try:
            while result_limit is None or records_fetched < result_limit:
                # Calculate how many records to fetch in this page
                if result_limit is not None:
                    records_to_fetch = min(page_size, result_limit - records_fetched)
                else:
                    records_to_fetch = page_size

                # Fetch contacts with pagination parameters
                response = api.get_contacts(
                    xero_tenant_id=self.handler.tenant_id,
                    summary_only=False,
                    page=page,
                    page_size=records_to_fetch,
                    **api_params
                )

                if not response.contacts:
                    break  # No more data

                all_data.extend(response.contacts)
                records_fetched += len(response.contacts)

                # Check pagination metadata to determine if there are more pages
                if hasattr(response, 'pagination') and response.pagination:
                    # If we've reached the last page, stop
                    if page >= response.pagination.page_count:
                        break
                else:
                    # Fallback: If we got fewer records than requested, we've reached the end
                    if len(response.contacts) < records_to_fetch:
                        break

                page += 1

        except Exception as e:
            raise Exception(f"Failed to fetch contacts: {str(e)}")

        # Convert all data to DataFrame
        df = self._convert_response_to_dataframe(all_data)
        if len(df) > 0:
            df.rename(columns=self.COLUMN_REMAP, inplace=True)

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Apply column selection
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)
        else:
            available_columns = [col for col in selected_columns if col in df.columns]
            df = df[available_columns]

        # Apply ordering
        if order_by_conditions:
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit (in case filters reduced the result set)
        if result_limit:
            df = df.head(result_limit)

        return df