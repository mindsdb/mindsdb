from typing import List

import pandas as pd
from mindsdb_sql.parser import ast

from mindsdb.integrations.handlers.utilities.query_utilities import (
    SELECTQueryExecutor,
    SELECTQueryParser,
)
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import conditions_to_filter


class CustomAPITable(APITable):
    name: str = None
    columns: List[str] = [
        "category",
        "percent",
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

        select_statement_parser = SELECTQueryParser(
            query, PyPIRecentTable.name, self.get_columns()
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            _,
        ) = select_statement_parser.parse_query()

        params = conditions_to_filter(query.where)

        raw_df = self.handler.connection.recent(params["package"], format="pandas")

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
        available_columns = (
            self.get_columns(["percent"])
            if "include_mirrors" in params
            else self.get_columns()
        )

        select_statement_parser = SELECTQueryParser(
            query,
            PyPIOverallTable.name,
            available_columns,
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            _,
        ) = select_statement_parser.parse_query()

        if "include_mirrors" in params:
            raw_df = self.handler.connection.overall(
                params["package"], format="pandas", mirrors=params["include_mirrors"]
            )
        else:
            raw_df = self.handler.connection.overall(params["package"], format="pandas")

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
        available_columns = (
            self.get_columns(["percent"]) if "version" in params else self.get_columns()
        )

        select_statement_parser = SELECTQueryParser(
            query,
            PyPIOverallTable.name,
            available_columns,
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            _,
        ) = select_statement_parser.parse_query()

        if "version" in params:
            raw_df = self.handler.connection.python_major(
                params["package"], format="pandas", version=params["version"]
            )
        else:
            raw_df = self.handler.connection.python_major(
                params["package"], format="pandas"
            )

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
        available_columns = (
            self.get_columns(["percent"]) if "version" in params else self.get_columns()
        )

        select_statement_parser = SELECTQueryParser(
            query,
            PyPIOverallTable.name,
            available_columns,
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            _,
        ) = select_statement_parser.parse_query()

        if "version" in params:
            raw_df = self.handler.connection.python_minor(
                params["package"], format="pandas", version=params["version"]
            )
        else:
            raw_df = self.handler.connection.python_minor(
                params["package"], format="pandas"
            )

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
        available_columns = (
            self.get_columns(["percent"]) if "os" in params else self.get_columns()
        )

        select_statement_parser = SELECTQueryParser(
            query,
            PyPIOverallTable.name,
            available_columns,
        )
        (
            selected_columns,
            _,
            order_by_conditions,
            _,
        ) = select_statement_parser.parse_query()

        if "os" in params:
            raw_df = self.handler.connection.system(
                params["package"], format="pandas", os=params["os"]
            )
        else:
            raw_df = self.handler.connection.system(params["package"], format="pandas")

        select_statement_executor = SELECTQueryExecutor(
            raw_df, selected_columns, [], order_by_conditions
        )

        result_df = select_statement_executor.execute_query()

        return result_df
