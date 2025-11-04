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

class OverpaymentsTable(XeroTable):
    """Table for Xero Overpayments"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "overpayment_id": {"type": "where", "xero_field": "OverpaymentID", "value_type": "guid"},
        "date": {"type": "where", "xero_field": "Date", "value_type": "date"},
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "line_amount_types": {"type": "where", "xero_field": "LineAmountTypes", "value_type": "string"},
        "sub_total": {"type": "where", "xero_field": "SubTotal", "value_type": "number"},
        "total_tax": {"type": "where", "xero_field": "TotalTax", "value_type": "number"},
        "total": {"type": "where", "xero_field": "Total", "value_type": "number"},
        "currency_code": {"type": "where", "xero_field": "CurrencyCode", "value_type": "string"},
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
        "currency_rate": {"type": "where", "xero_field": "CurrencyRate", "value_type": "number"},
        "remaining_credit": {"type": "where", "xero_field": "RemainingCredit", "value_type": "number"},
    }
    
    COLUMN_REMAP = {
        "contact_contact_id": "contact_id"
    }

    def get_columns(self) -> List[str]:
        return [
            "overpayment_id",
            "contact_id",
            "contact_name",
            "date_string",
            "date",
            "status",
            "line_amount_types",
            "sub_total",
            "total_tax",
            "total",
            "updated_date_utc",
            "currency_code",
            "type",
            "currency_rate",
            "remaining_credit",
            "allocations",
            "has_attachments"
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
            print("API Params:", api_params)
            # Fetch overpayments with optimized parameters
            overpayments = api.get_overpayments(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(overpayments.overpayments or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch overpayments: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "overpayments", columns=self.get_columns()
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