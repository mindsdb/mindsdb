from typing import List
from datetime import datetime, timedelta, date
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


class BankSummaryReportTable(XeroReportTable):
    """Table for Xero Bank Summary Report"""

    # Define which parameters can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "from_date": {"type": "direct", "param": "from_date"},
        "to_date": {"type": "direct", "param": "to_date"},
    }

    COLUMN_REMAP = {}

    def get_columns(self) -> List[str]:
        """
        Return column names for the bank summary report.

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
        Fetch bank summary report from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Parse query to get result_limit
        parser = SELECTQueryParser(
            query, "bank_summary_report", columns=self.get_columns()
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

        # Set default dates if not provided
        # from_date defaults to beginning of current month
        # to_date defaults to end of current month
        now = datetime.now()

        if 'from_date' not in api_params:
            # First day of current month as date object
            api_params['from_date'] = date(now.year, now.month, 1)
        else:
            api_params['from_date'] = self._convert_date_parameter(api_params['from_date'])

        if 'to_date' not in api_params:
            # Last day of current month as date object
            # Get first day of next month, then subtract one day
            if now.month == 12:
                next_month = date(now.year + 1, 1, 1)
            else:
                next_month = date(now.year, now.month + 1, 1)
            api_params['to_date'] = next_month - timedelta(days=1)
        else:
            api_params['to_date'] = self._convert_date_parameter(api_params['to_date'])

        try:
            # Fetch bank summary report
            response = api.get_report_bank_summary(
                xero_tenant_id=self.handler.tenant_id,
                **api_params
            )

            # Parse report structure to DataFrame
            df = self._parse_report_to_dataframe(response)

        except Exception as e:
            raise Exception(f"Failed to fetch bank summary report: {str(e)}")

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
