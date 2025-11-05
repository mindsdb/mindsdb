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

class PurchaseOrdersTable(XeroTable):
    """Table for Xero Purchase Orders"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "status": {"type": "direct", "param": "status", "value_type": "string"},
        "date": {"type": "date", "param": "date_from", "param_upper": "date_to"},
    }

    COLUMN_REMAP = {
        # Contact nested fields
        "contact_contact_id": "contact_id",
        "contact_name": "contact_name",
        "contact_contact_status": "contact_status",
        "contact_addresses": "contact_addresses",
        "contact_phones": "contact_phones",
        "contact_updated_date_utc": "contact_updated_date_utc",
        "contact_contact_groups": "contact_groups",
        "contact_default_currency": "contact_default_currency",
        "contact_contact_persons": "contact_persons",
        "contact_has_validation_errors": "contact_has_validation_errors",
    }

    def get_columns(self) -> List[str]:
        return [
            # Core purchase order fields
            "purchase_order_id",
            "purchase_order_number",
            "reference",
            "status",
            "type",

            # Date fields
            "date",
            "date_string",
            "delivery_date",
            "delivery_date_string",
            "expected_arrival_date",

            # Financial fields
            "currency_code",
            "currency_rate",
            "sub_total",
            "total_tax",
            "total",
            "total_discount",
            "line_amount_types",

            # Contact nested fields
            "contact_id",
            "contact_name",
            "contact_status",
            "contact_default_currency",
            "contact_has_validation_errors",

            # Contact list fields (JSON strings)
            "contact_addresses",
            "contact_phones",
            "contact_groups",
            "contact_persons",

            # Delivery information
            "delivery_address",
            "attention_to",
            "telephone",
            "delivery_instructions",
            "sent_to_contact",

            # Line items (JSON string)
            "line_items",

            # Metadata
            "branding_theme_id",
            "has_attachments",
            "has_errors",
            "is_discounted",
            "updated_date_utc",
            "status_attribute_string",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch purchase orders from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Parse query to get result_limit (no default limit)
        parser = SELECTQueryParser(
            query, "purchase_orders", columns=self.get_columns(), use_default_limit=False
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

                # Fetch purchase orders with pagination parameters
                response = api.get_purchase_orders(
                    xero_tenant_id=self.handler.tenant_id,
                    page=page,
                    page_size=records_to_fetch,
                    **api_params
                )

                if not response.purchase_orders:
                    break  # No more data

                all_data.extend(response.purchase_orders)
                records_fetched += len(response.purchase_orders)

                # Check pagination metadata to determine if there are more pages
                if hasattr(response, 'pagination') and response.pagination:
                    # If we've reached the last page, stop
                    if page >= response.pagination.page_count:
                        break
                else:
                    # Fallback: If we got fewer records than requested, we've reached the end
                    if len(response.purchase_orders) < records_to_fetch:
                        break

                page += 1

        except Exception as e:
            raise Exception(f"Failed to fetch purchase orders: {str(e)}")

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
