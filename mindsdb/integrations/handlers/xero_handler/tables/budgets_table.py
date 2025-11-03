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

class BudgetsTable(XeroTable):
    """Table for Xero Budgets"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "budget_id": {"type": "where", "xero_field": "BudgetID", "value_type": "guid"},
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
        "date": {"type": "date", "param": "date_from", "param_upper": "date_to"},
    }
    
    COLUMN_REMAP = {}

    def get_columns(self) -> List[str]:
        return [
            "budget_id",
            "status",
            "description",
            "tracking",
            "budget_lines",
            "type",
            "updated_date_utc",
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
            # Fetch budgets with optimized parameters
            budgets = api.get_budgets(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(budgets.budgets or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch budgets: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "budgets", columns=self.get_columns()
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