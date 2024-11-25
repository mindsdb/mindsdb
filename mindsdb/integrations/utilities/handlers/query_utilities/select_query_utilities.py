from typing import Text, List, Dict, Tuple

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.sql_utils import sort_dataframe
from mindsdb.integrations.utilities.handlers.query_utilities.base_query_utilities import BaseQueryParser
from mindsdb.integrations.utilities.handlers.query_utilities.base_query_utilities import BaseQueryExecutor


class SELECTQueryParser(BaseQueryParser):
    """
    Parses a SELECT query into its component parts.

    Parameters
    ----------
    query : ast.Select
        Given SQL SELECT query.
    table : Text
        Name of the table to query.
    columns : List[Text]
        List of columns in the table.
    """
    def __init__(self, query: ast.Select, table: Text, columns: List[Text]):
        super().__init__(query)
        self.table = table
        self.columns = columns

    def parse_query(self) -> Tuple[List[Text], List[List[Text]], Dict[Text, List[Text]], int]:
        """
        Parses a SQL SELECT statement into its components: SELECT, WHERE, ORDER BY, LIMIT.
        """
        selected_columns = self.parse_select_clause()
        where_conditions = self.parse_where_clause()
        order_by_conditions = self.parse_order_by_clause()
        result_limit = self.parse_limit_clause()

        return selected_columns, where_conditions, order_by_conditions, result_limit

    def parse_select_clause(self) -> List[Text]:
        """
        Parses the SELECT (column selection) clause of the query.
        """
        selected_columns = []
        for target in self.query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.columns
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        return selected_columns

    def parse_order_by_clause(self) -> Dict[Text, List[Text]]:
        """
        Parses the ORDER BY clause of the query.
        """
        if self.query.order_by and len(self.query.order_by) > 0:
            return self.query.order_by
        else:
            return []

    def parse_limit_clause(self) -> int:
        """
        Parses the LIMIT clause of the query.
        """
        if self.query.limit:
            result_limit = self.query.limit.value
        else:
            result_limit = 20

        return result_limit


class SELECTQueryExecutor(BaseQueryExecutor):
    """
    Executes a SELECT query.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to query.
    selected_columns : List[Text]
        List of columns to select.
    where_conditions : List[List[Text]]
        List of where conditions.
    order_by_conditions : Dict[Text, List[Text]]
        Dictionary of order by conditions.
    result_limit : int
        Number of results to return.
    """
    def __init__(self, df: pd.DataFrame, selected_columns: List[Text], where_conditions: List[List[Text]], order_by_conditions: List, result_limit: int = None):
        super().__init__(df, where_conditions)
        self.selected_columns = selected_columns
        self.order_by_conditions = order_by_conditions
        self.result_limit = result_limit

    def execute_query(self):
        """
        Execute the query.
        """
        self.execute_limit_clause()

        self.execute_where_clause()

        self.execute_select_clause()

        self.execute_order_by_clause()

        return self.df

    def execute_select_clause(self):
        """
        Execute the select clause of the query.
        """
        if len(self.df) == 0:
            self.df = pd.DataFrame([], columns=self.selected_columns)
        else:
            self.df = self.df[self.selected_columns]

    def execute_order_by_clause(self):
        """
        Execute the order by clause of the query.
        """
        self.df = sort_dataframe(self.df, self.order_by_conditions)

    def execute_limit_clause(self):
        """
        Execute the limit clause of the query.
        """
        if self.result_limit:
            self.df = self.df.head(self.result_limit)
