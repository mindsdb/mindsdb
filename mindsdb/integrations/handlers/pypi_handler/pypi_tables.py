from typing import List

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryExecutor,
    SELECTQueryParser,
)
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import conditions_to_filter


class CustomAPITable(APITable):
    name: str = None
    columns: List[str] = [
        "category",
        "date",
        "downloads",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """

        return [item for item in self.columns if item not in ignore]


class PyPIRecentTable(CustomAPITable):
    name: str = "recent"
    columns: List[str] = [
        "last_day",
        "last_week",
        "last_month",
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """triggered at the SELECT query

        Args:
            query (ast.Select): user's entered query

        Returns:
            pd.DataFrame: the queried information
        """
        params = conditions_to_filter(query.where)

        package_name = params["package"]
        period = params.get("period", None)
        all_cols = {
            "day": "last_day",
            "week": "last_week",
            "month": "last_month",
        }

        to_be_excluded = []

        if period:
            if period in all_cols.keys():
                del all_cols[period]
                to_be_excluded = list(all_cols.values())
            else:
                raise ValueError(
                    "Make sure that one of `day`, `week` or `month` values is assigned to `period`."
                )

        select_statement_parser = SELECTQueryParser(
            query, PyPIRecentTable.name, self.get_columns(to_be_excluded)
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            _,
        ) = select_statement_parser.parse_query()

        raw_df = self.handler.connection(name=package_name).recent(period)

        select_statement_executor = SELECTQueryExecutor(
            raw_df, selected_columns, [], order_by_conditions
        )

        result_df = select_statement_executor.execute_query()

        return result_df


class PyPIOverallTable(CustomAPITable):
    name: str = "overall"

    def select(self, query: ast.Select) -> pd.DataFrame:
        """triggered at the SELECT query

        Args:
            query (ast.Select): user's entered query

        Returns:
            pd.DataFrame: the queried information
        """
        params = conditions_to_filter(query.where)

        package_name = params["package"]
        mirrors = params.get("mirrors", None)

        select_statement_parser = SELECTQueryParser(
            query,
            PyPIOverallTable.name,
            self.get_columns(),
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        raw_df = self.handler.connection(name=package_name, limit=result_limit).overall(
            mirrors=mirrors
        )

        select_statement_executor = SELECTQueryExecutor(
            raw_df, selected_columns, [], order_by_conditions
        )

        result_df = select_statement_executor.execute_query()

        return result_df


class PyPIPythonMajorTable(CustomAPITable):
    name: str = "python_major"

    def select(self, query: ast.Select) -> pd.DataFrame:
        """triggered at the SELECT query

        Args:
            query (ast.Select): user's entered query

        Returns:
            pd.DataFrame: the queried information
        """
        params = conditions_to_filter(query.where)

        package_name = params["package"]
        version = params.get("version", None)

        select_statement_parser = SELECTQueryParser(
            query,
            PyPIOverallTable.name,
            self.get_columns(),
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        raw_df = self.handler.connection(
            name=package_name, limit=result_limit
        ).python_major(version=version)

        select_statement_executor = SELECTQueryExecutor(
            raw_df, selected_columns, [], order_by_conditions
        )

        result_df = select_statement_executor.execute_query()

        return result_df


class PyPIPythonMinorTable(CustomAPITable):
    name: str = "python_minor"

    def select(self, query: ast.Select) -> pd.DataFrame:
        """triggered at the SELECT query

        Args:
            query (ast.Select): user's entered query

        Returns:
            pd.DataFrame: the queried information
        """
        params = conditions_to_filter(query.where)

        package_name = params["package"]
        version = params.get("version", None)

        select_statement_parser = SELECTQueryParser(
            query,
            PyPIOverallTable.name,
            self.get_columns(),
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        raw_df = self.handler.connection(
            name=package_name, limit=result_limit
        ).python_minor(version=version)

        select_statement_executor = SELECTQueryExecutor(
            raw_df, selected_columns, [], order_by_conditions
        )

        result_df = select_statement_executor.execute_query()

        return result_df


class PyPISystemTable(CustomAPITable):
    name: str = "system"

    def select(self, query: ast.Select) -> pd.DataFrame:
        """triggered at the SELECT query

        Args:
            query (ast.Select): user's entered query

        Returns:
            pd.DataFrame: the queried information
        """
        params = conditions_to_filter(query.where)

        package_name = params["package"]
        os = params.get("os", None)

        select_statement_parser = SELECTQueryParser(
            query,
            PyPIOverallTable.name,
            self.get_columns(),
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        raw_df = self.handler.connection(name=package_name, limit=result_limit).system(
            os=os
        )

        select_statement_executor = SELECTQueryExecutor(
            raw_df, selected_columns, [], order_by_conditions
        )

        result_df = select_statement_executor.execute_query()

        return result_df
