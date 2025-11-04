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

class PaymentsTable(XeroTable):
    """Table for Xero Payments"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "payment_id": {"type": "where", "xero_field": "PaymentID", "value_type": "guid"},
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "date": {"type": "where", "xero_field": "Date", "value_type": "date"},
        "invoice_id": {"type": "where", "xero_field": "Invoice.InvoiceID", "value_type": "guid"},
        "reference": {"type": "where", "xero_field": "Reference", "value_type": "string"},
        "contact_id": {"type": "where", "xero_field": "Invoice.Contact.ContactID", "value_type": "guid"},
    }
    
    COLUMN_REMAP = {
        # Account fields (top-level)
        "account_account_id": "account_id",
        "account_code": "account_code",

        # BatchPayment nested fields
        "batch_payment_batch_payment_id": "batch_payment_id",
        "batch_payment_account_account_id": "batch_payment_account_id",
        "batch_payment_account_code": "batch_payment_account_code",
        "batch_payment_date_string": "batch_payment_date_string",
        "batch_payment_date": "batch_payment_date",
        "batch_payment_type": "batch_payment_type",
        "batch_payment_status": "batch_payment_status",
        "batch_payment_total_amount": "batch_payment_total_amount",
        "batch_payment_updated_date_utc": "batch_payment_updated_date_utc",
        "batch_payment_is_reconciled": "batch_payment_is_reconciled",

        # Invoice nested fields
        "invoice_invoice_id": "invoice_id",
        "invoice_invoice_number": "invoice_number",
        "invoice_type": "invoice_type",
        "invoice_currency_code": "invoice_currency_code",
        "invoice_contact_contact_id": "invoice_contact_id",
        "invoice_contact_name": "invoice_contact_name",
        "invoice_payments": "invoice_payments",
        "invoice_credit_notes": "invoice_credit_notes",
        "invoice_prepayments": "invoice_prepayments",
        "invoice_overpayments": "invoice_overpayments",
        "invoice_line_items": "invoice_line_items",
        "invoice_invoice_addresses": "invoice_addresses",
        "invoice_contact_contact_groups": "invoice_contact_groups",
        "invoice_contact_contact_persons": "invoice_contact_persons",
        
    }

    def get_columns(self) -> List[str]:
        return [
            # Core payment fields
            "payment_id",
            "date",
            "bank_amount",
            "amount",
            "reference",
            "currency_rate",
            "payment_type",
            "status",
            "updated_date_utc",
            "has_account",
            "is_reconciled",
            "has_validation_errors",

            # Account fields
            "account_id",
            "account_code",

            # BatchPayment nested fields
            "batch_payment_id",
            "batch_payment_account_id",
            "batch_payment_account_code",
            "batch_payment_date_string",
            "batch_payment_date",
            "batch_payment_type",
            "batch_payment_status",
            "batch_payment_total_amount",
            "batch_payment_updated_date_utc",
            "batch_payment_is_reconciled",

            # Invoice nested fields
            "invoice_id",
            "invoice_number",
            "invoice_type",
            "invoice_currency_code",
            "invoice_is_discounted",
            "invoice_contact_id",
            "invoice_contact_name",
            "invoice_contact_addresses",
            "invoice_contact_phones",
            "invoice_contact_groups",
            "invoice_contact_persons",
            "invoice_contact_has_validation_errors",

            # Invoice list fields (JSON strings)
            "invoice_payments",
            "invoice_credit_notes",
            "invoice_prepayments",
            "invoice_overpayments",
            "invoice_line_items",
            "invoice_addresses",
            "invoice_payment_services",
            
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
            query, "payments", columns=self.get_columns(), use_default_limit=False
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
        page_size = 10  # Xero's maximum page size
        records_fetched = 0

        try:
            while result_limit is None or records_fetched < result_limit:
                # Calculate how many records to fetch in this page
                if result_limit is not None:
                    records_to_fetch = min(page_size, result_limit - records_fetched)
                else:
                    records_to_fetch = page_size

                # Fetch payments with pagination parameters
                response = api.get_payments(
                    xero_tenant_id=self.handler.tenant_id,
                    page=page,
                    page_size=records_to_fetch,
                    **api_params
                )

                if not response.payments:
                    break  # No more data

                all_data.extend(response.payments)
                records_fetched += len(response.payments)

                # Check pagination metadata to determine if there are more pages
                if hasattr(response, 'pagination') and response.pagination:
                    # If we've reached the last page, stop
                    if page >= response.pagination.page_count:
                        break
                else:
                    # Fallback: If we got fewer records than requested, we've reached the end
                    if len(response.payments) < records_to_fetch:
                        break

                page += 1

        except Exception as e:
            raise Exception(f"Failed to fetch payments: {str(e)}")

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