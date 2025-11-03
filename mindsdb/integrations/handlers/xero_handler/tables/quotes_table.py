from typing import List
import pandas as pd
from mindsdb_sql_parser import ast
from mindsdb.integrations.handlers.xero_handler.xero_tables import XeroTable, extract_comparison_conditions
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser
from xero_python.accounting import AccountingApi


class QuotesTable(XeroTable):
    """Table for Xero Quotes"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "quote_number": {"type": "direct", "param": "quote_number"},
        "status": {"type": "direct", "param": "status"},
        "date": {"type": "date", "param": "date_from", "param_upper": "date_to"},
        "expiry_date": {"type": "date", "param": "expiry_date_from", "param_upper": "expiry_date_to"},
        "contact_id": {"type": "direct", "param": "contact_id"}
    }

    def get_columns(self) -> List[str]:
        return [
            "quote_id",
            "quote_number",
            "reference",
            "terms",
            "contact",
            "line_items",
            "date",
            "date_string",
            "expiry_date",
            "expiry_date_string",
            "status",
            "currency_rate",
            "currency_code",
            "sub_total",
            "total_tax",
            "total",
            "total_discount",
            "title",
            "summary",
            "branding_theme_id",
            "updated_date_utc",
            "line_amount_types"
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch quotes from Xero API

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
            # Fetch quotes with optimized parameters
            quotes = api.get_quotes(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(quotes.quotes or [])
        except Exception as e:
            raise Exception(f"Failed to fetch quotes: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            from mindsdb.integrations.utilities.sql_utils import filter_dataframe
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "quotes", columns=self.get_columns()
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
            from mindsdb.integrations.utilities.sql_utils import sort_dataframe
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df