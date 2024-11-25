import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities import log
from mindsdb_sql_parser import ast

logger = log.getLogger(__name__)


class OilPriceLatestTable(APITable):
    """The Latest Oil Price Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.oilpriceapi.com/guide/#prices-latest" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            latest oil price matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'latest_price',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []

        for op, arg1, arg2 in where_conditions:
            if arg1 == 'by_type':
                if op == '=':
                    search_params["by_type"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for by_type column.")

                if not self.handler.client._is_valid_by_type(arg2):
                    raise ValueError("Unknown value for `by_type` parameter. The allowed values are - " + self.handler.client.valid_values_by_type)

            elif arg1 == 'by_code':
                if op == '=':
                    search_params["by_code"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for by_code column.")

                if not self.handler.client._is_valid_by_code(arg2):
                    raise ValueError("Unknown value for `by_code` parameter. The allowed values are - " + self.handler.client.valid_values_by_code)

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        latest_price_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.get_latest_price(search_params.get("by_type"), search_params.get("by_code"))

        self.check_res(res=response)

        content = response["content"]

        latest_price_df = pd.json_normalize(content["data"])

        select_statement_executor = SELECTQueryExecutor(
            latest_price_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        latest_price_df = select_statement_executor.execute_query()

        return latest_price_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "price",
            "formatted",
            "currency",
            "code",
            "created_at",
            "type"
        ]


class OilPricePastDayPriceTable(APITable):
    """The Past Day Oil Price Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.oilpriceapi.com/guide/#prices-past-day" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            past day oil price matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'past_day_price',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []

        for op, arg1, arg2 in where_conditions:
            if arg1 == 'by_type':
                if op == '=':
                    search_params["by_type"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for by_type column.")

                if not self.handler.client._is_valid_by_type(arg2):
                    raise ValueError("Unknown value for `by_type` parameter. The allowed values are - " + self.handler.client.valid_values_by_type)

            elif arg1 == 'by_code':
                if op == '=':
                    search_params["by_code"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for by_code column.")

                if not self.handler.client._is_valid_by_code(arg2):
                    raise ValueError("Unknown value for `by_code` parameter. The allowed values are - " + self.handler.client.valid_values_by_code)

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        price_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.get_price_past_day(search_params.get("by_type"), search_params.get("by_code"))

        self.check_res(res=response)

        content = response["content"]

        price_df = pd.json_normalize(content["data"]["prices"])

        select_statement_executor = SELECTQueryExecutor(
            price_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        price_df = select_statement_executor.execute_query()

        return price_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "price",
            "formatted",
            "currency",
            "code",
            "created_at",
            "type"
        ]
