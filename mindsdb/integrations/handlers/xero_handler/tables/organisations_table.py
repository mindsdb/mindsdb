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

class OrganisationsTable(XeroTable):
    """Table for Xero Organisations"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
    }
    
    COLUMN_REMAP = {}

    def get_columns(self) -> List[str]:
        return [
            "organisation_id",
            "name",
            "legal_name",
            "pays_tax",
            "version",
            "organisation_type",
            "base_currency",
            "country_code",
            "is_demo_company",
            "organisation_status",
            "tax_number",
            "financial_year_end_day",
            "financial_year_end_month",
            "sales_tax_basis",
            "sales_tax_period",
            "default_sales_tax",
            "default_purchases_tax",
            "period_lock_date",
            "created_date_utc",
            "organisation_entity_type",
            "timezone",
            "short_code",
            "edition",
            "class",
            "addresses",
            "phones",
            "external_links",
            "payment_terms",
            "tax_number_name"
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
            # Fetch organisations with optimized parameters
            organisations = api.get_organisations(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(organisations.organisations or [])
            df.rename(columns=self.COLUMN_REMAP, inplace=True)
        except Exception as e:
            raise Exception(f"Failed to fetch organisations: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "organisations", columns=self.get_columns()
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