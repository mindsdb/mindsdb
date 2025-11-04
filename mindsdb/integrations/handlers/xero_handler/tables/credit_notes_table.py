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

class CreditNotesTable(XeroTable):
    """Table for Xero Credit Notes"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "credit_note_id": {"type": "where", "xero_field": "CreditNoteID", "value_type": "guid"},
        "credit_note_number": {"type": "where", "xero_field": "CreditNoteNumber", "value_type": "string"},
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
        "reference": {"type": "where", "xero_field": "Reference", "value_type": "string"},
        "remaining_credit": {"type": "param", "param": "SearchTerm", "value_type": "string"},
        "date": {"type": "where", "xero_field": "Date", "value_type": "date"},
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "line_amount_types": {"type": "where", "xero_field": "LineAmountTypes", "value_type": "string"},
        "sub_total": {"type": "where", "xero_field": "SubTotal", "value_type": "number"},
        "total_tax": {"type": "where", "xero_field": "TotalTax", "value_type": "number"},
        "total": {"type": "where", "xero_field": "Total", "value_type": "number"},
        "updated_date_utc": {"type": "where", "xero_field": "UpdatedDateUTC", "value_type": "date"},
        "currency_code": {"type": "where", "xero_field": "CurrencyCode", "value_type": "string"},
        "fully_paid_on_date": {"type": "where", "xero_field": "FullyPaidOnDate", "value_type": "date"},
    }
    
    COLUMN_REMAP = {
        "contact_contact_id": "contact_id",
        "contact_contact_persons": "contact_persons",
        "contact_contact_groups": "contact_groups",
    }

    def get_columns(self) -> List[str]:
        return [
            "credit_note_id",
            "credit_note_number",
            "payments",
            "has_errors",
            "invoice_addresses",
            "type",
            "reference",
            "remaining_credit",
            "allocations",
            "has_attachments",
            "contact_id",
            "contact_name",
            "contact_addresses",
            "contact_phones",
            "contact_groups",
            "contact_persons",
            "contact_has_validation_errors",
            "date_string",
            "date",
            "status",
            "line_amount_types",
            "line_items",
            "sub_total",
            "total_tax",
            "total",
            "updated_date_utc",
            "currency_code",
            "fully_paid_on_date"
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
            # Fetch credit notes with optimized parameters
            credit_notes = api.get_credit_notes(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(credit_notes.credit_notes or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch credit notes: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "credit_notes", columns=self.get_columns()
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