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

class ManualJournalsTable(XeroTable):
    """Table for Xero Manual Journals"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "manual_journal_id": {"type": "where", "xero_field": "ManualJournalID", "value_type": "guid"},
        "date": {"type": "where", "xero_field": "Date", "value_type": "date"},
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "line_amount_types": {"type": "where", "xero_field": "LineAmountTypes", "value_type": "string"},
        "show_on_cash_basis_reports": {"type": "where", "xero_field": "ShowOnCashBasisReports", "value_type": "bool"},
    }
    
    COLUMN_REMAP = {}

    def get_columns(self) -> List[str]:
        return [
            "manual_journal_id",
            "date",
            "line_amount_types",
            "status",
            "narration",
            "journal_lines",
            "show_on_cash_basis_reports",
            "has_attachments",
            "updated_date_utc"
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
            # Fetch journals with optimized parameters
            manual_journals = api.get_manual_journals(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(manual_journals.manual_journals or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch manual journals: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "manual_journals", columns=self.get_columns()
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