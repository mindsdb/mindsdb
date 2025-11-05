from typing import List
import pandas as pd
from mindsdb_sql_parser import ast
from mindsdb.integrations.handlers.xero_handler.xero_report_tables import XeroReportTable
from mindsdb.integrations.handlers.xero_handler.xero_tables import (
    extract_comparison_conditions,
    filter_dataframe,
    sort_dataframe
)
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser
from xero_python.accounting import AccountingApi


class ProfitLossReportTable(XeroReportTable):
    """Table for Xero Profit and Loss Report"""

    # Define which parameters can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "from_date": {"type": "direct", "param": "from_date"},
        "to_date": {"type": "direct", "param": "to_date"},
        "periods": {"type": "direct", "param": "periods"},
        "timeframe": {"type": "direct", "param": "timeframe"},
        "tracking_category_id": {"type": "direct", "param": "tracking_category_id"},
        "tracking_option_id": {"type": "direct", "param": "tracking_option_id"},
        "tracking_category_id_2": {"type": "direct", "param": "tracking_category_id2"},
        "tracking_option_id_2": {"type": "direct", "param": "tracking_option_id2"},
        "standard_layout": {"type": "direct", "param": "standard_layout"},
        "payments_only": {"type": "direct", "param": "payments_only"},
    }

    COLUMN_REMAP = {}

    def get_columns(self) -> List[str]:
        """
        Return column names for the profit and loss report.

        Returns:
            List[str]: Column names
        """
        base_columns = [
            "report_id",
            "report_name",
            "report_title",
            "report_type",
            "report_date",
            "updated_date_utc",
            "section",
            "subsection",
            "depth",
            "row_type",
            "row_title",
            "account_id",
        ]

        # Add generic period columns
        period_columns = [f"period_{i}" for i in range(1, self.MAX_PERIODS + 1)]

        return base_columns + period_columns

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch profit and loss report from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Parse query to get result_limit
        parser = SELECTQueryParser(
            query, "profit_loss_report", columns=self.get_columns()
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

        # Convert date parameters to proper format
        if 'from_date' in api_params:
            api_params['from_date'] = self._convert_date_parameter(api_params['from_date'])
        if 'to_date' in api_params:
            api_params['to_date'] = self._convert_date_parameter(api_params['to_date'])

        try:
            # Fetch profit and loss report
            response = api.get_report_profit_and_loss(
                xero_tenant_id=self.handler.tenant_id,
                **api_params
            )

            # Parse report structure to DataFrame
            df = self._parse_report_to_dataframe(response)

        except Exception as e:
            raise Exception(f"Failed to fetch profit and loss report: {str(e)}")

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

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df
