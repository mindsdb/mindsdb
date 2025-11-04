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

class RepeatingInvoicesTable(XeroTable):
    """Table for Xero Repeating Invoices"""

    # Define which columns can be pushed to the Xero API
    # Note: Repeating invoices only support WHERE clause filtering, no dedicated params
    SUPPORTED_FILTERS = {
        "repeating_invoice_id": {"type": "where", "xero_field": "RepeatingInvoiceID", "value_type": "guid"},
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "reference": {"type": "where", "xero_field": "Reference", "value_type": "string"},
        "approved_for_sending": {"type": "where", "xero_field": "ApprovedForSending", "value_type": "boolean"},
        "line_amount_types": {"type": "where", "xero_field": "LineAmountTypes", "value_type": "string"},
        "total": {"type": "where", "xero_field": "Total", "value_type": "number"},
        "sub_total": {"type": "where", "xero_field": "SubTotal", "value_type": "number"},
        "total_tax": {"type": "where", "xero_field": "TotalTax", "value_type": "number"},
        "currency_code": {"type": "where", "xero_field": "CurrencyCode", "value_type": "string"},
        
        # Contact nested fields
        "contact_id": {"type": "where", "xero_field": "Contact.ContactID", "value_type": "guid"},
        "contact_name": {"type": "where", "xero_field": "Contact.Name", "value_type": "string"},
        
        # Schedule nested fields
        "schedule_unit": {"type": "where", "xero_field": "Schedule.Unit", "value_type": "string"},
        "schedule_due_date": {"type": "where", "xero_field": "Schedule.DueDate", "value_type": "number"},
        "schedule_due_date_type": {"type": "where", "xero_field": "Schedule.DueDateType", "value_type": "string"},
        "schedule_next_scheduled_date": {"type": "where", "xero_field": "Schedule.NextScheduledDate", "value_type": "date"},

    }

    COLUMN_REMAP = {
        # Contact nested fields
        "contact_contact_id": "contact_id",
        "contact_name": "contact_name",
        "contact_addresses": "contact_addresses",
        "contact_phones": "contact_phones",
        "contact_contact_groups": "contact_groups",
        "contact_contact_persons": "contact_persons",
        "contact_has_validation_errors": "contact_has_validation_errors",

        # Schedule nested fields
        "schedule_period": "schedule_period",
        "schedule_unit": "schedule_unit",
        "schedule_due_date": "schedule_due_date",
        "schedule_due_date_type": "schedule_due_date_type",
        "schedule_start_date": "schedule_start_date",
        "schedule_next_scheduled_date": "schedule_next_scheduled_date",
        "schedule_end_date": "schedule_end_date",
    }

    def get_columns(self) -> List[str]:
        return [
            # Core repeating invoice fields
            "repeating_invoice_id",
            "id",
            "type",
            "status",
            "reference",

            # Financial fields
            "currency_code",
            "line_amount_types",
            "sub_total",
            "total_tax",
            "total",

            # Contact nested fields
            "contact_id",
            "contact_name",
            "contact_has_validation_errors",

            # Contact list fields (JSON strings)
            "contact_addresses",
            "contact_phones",
            "contact_groups",
            "contact_persons",

            # Schedule nested fields
            "schedule_period",
            "schedule_unit",
            "schedule_due_date",
            "schedule_due_date_type",
            "schedule_start_date",
            "schedule_next_scheduled_date",
            "schedule_end_date",

            # Line items (JSON string)
            "line_items",

            # Metadata
            "branding_theme_id",
            "has_attachments",
            "approved_for_sending",
            "send_copy",
            "mark_as_sent",
            "include_pdf",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch repeating invoices from Xero API

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
            # Fetch repeating invoices with WHERE clause if provided
            repeating_invoices = api.get_repeating_invoices(
                xero_tenant_id=self.handler.tenant_id,
                **api_params
            )
            df = self._convert_response_to_dataframe(repeating_invoices.repeating_invoices or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch repeating invoices: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "repeating_invoices", columns=self.get_columns()
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
