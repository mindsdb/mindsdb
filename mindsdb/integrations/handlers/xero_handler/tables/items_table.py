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

class ItemsTable(XeroTable):
    """Table for Xero Items"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "item_id": {"type": "where", "xero_field": "ItemID", "value_type": "guid"},
        "code": {"type": "where", "xero_field": "Type", "value_type": "string"},
        "name": {"type": "where", "xero_field": "Name", "value_type": "string"},
        "is_tracked_as_inventory": {"type": "where", "xero_field": "IsTrackedAsInventory", "value_type": "bool"},
        "is_sold": {"type": "where", "xero_field": "IsSold", "value_type": "bool"},
        "is_purchased": {"type": "where", "xero_field": "IsPurchased", "value_type": "bool"}
    }
    
    COLUMN_REMAP = {}

    def get_columns(self) -> List[str]:
        return [
            "item_id",
            "code",
            "description",
            "purchase_description",
            "purchase_details",
            "updated_date_utc",
            "sales_details_unit_price",
            "sales_details_account_code",
            "sales_details_tax_type",
            "name",
            "is_tracked_as_inventory",
            "is_sold",
            "is_purchased",
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
            # Fetch items with optimized parameters
            items = api.get_items(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(items.items or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch items: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "items", columns=self.get_columns()
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