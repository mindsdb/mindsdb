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

class InvoicesTable(XeroTable):
    """Table for Xero Invoices"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "invoice_id": {"type": "id_list", "param": "i_ds"},
        # "invoice_number": {"type": "id_list", "param": "invoice_numbers"},
        "invoice_number": {"type": "direct", "param": "search_term", "value_type": "string"},
        "contact_id": {"type": "id_list", "param": "contact_i_ds"},
        "status": {"type": "id_list", "param": "statuses"},
        "contact_name": {"type": "where", "xero_field": "Contact.Name", "value_type": "string"},
        "contact_number": {"type": "where", "xero_field": "Contact.ContactNumber", "value_type": "string"},
        "reference": {"type": "direct", "param": "search_term", "value_type": "string"},
        "date": {"type": "where", "xero_field": "Date", "value_type": "date"},
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
        "amount_due": {"type": "where", "xero_field": "AmountDue", "value_type": "number"},
        "amount_paid": {"type": "where", "xero_field": "AmountPaid", "value_type": "number"},
        "due_date": {"type": "where", "xero_field": "DueDate", "value_type": "date"}
    }
    
    COLUMN_REMAP = {
        "contact_contact_id": "contact_id",
        "contact_contact_persons": "contact_persons",
        "contact_contact_groups": "contact_groups",
    }

    def get_columns(self) -> List[str]:
        return [
            "invoice_id",
            "invoice_number",
            "reference",
            "payments",
            "credit_notes",
            "type",
            "pre_payments",
            "over_payments",
            "amount_due",
            "amount_paid",
            "amount_credited",
            "currency_rate",
            "is_discounted",
            "has_attachments",
            "invoice_addresses",
            "hasErrors",
            "invoice_payment_services",
            "contact_id",
            "contact_name",
            "contact_addresses",
            "contact_phones",
            "contact_groups",
            "contact_persons",
            "contact_has_validation_errors",
            "date",
            "date_string",
            "due_date",
            "due_date_string",
            "branding_theme_id",
            "status",
            "line_amount_types",
            "line_items",
            "sub_total",
            "total_tax",
            "total",
            "updated_date_utc",
            "currency_code"
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
            # Fetch invoices with optimized parameters
            invoices = api.get_invoices(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(invoices.invoices or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch invoices: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "invoices", columns=self.get_columns()
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