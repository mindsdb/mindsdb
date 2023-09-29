import pandas as pd
from typing import Text, List
from mindsdb_sql.parser import ast
from abc import ABC, abstractmethod
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class BaseQueryParser(ABC):
    """
    Parses a SQL query into its component parts.

    Parameters
    ----------
    query : ast
        Given SQL query.
    """

    def __init__(self, query: ast):
        self.query = query

    @abstractmethod
    def parse_query(self):
        """
        Parses a SQL statement into its components.
        """
        pass

    def parse_where_clause(self) -> List[List[Text]]:
        """
        Parses the WHERE clause of the query.
        """
        where_conditions = extract_comparison_conditions(self.query.where)
        return where_conditions
    

class BaseQueryExecutor(ABC):
    """
    Executes a SQL query.

    Parameters
    ----------
    query : ast
        Given SQL query.
    """

    def __init__(self, df: pd.DataFrame, where_conditions: List[List[Text]]):
        self.df = df
        self.where_conditions = where_conditions

    def execute_query(self):
        """
        Executes the SQL query.
        """
        self.execute_where_clause()

        return self.df

    def execute_where_clause(self):
        """
        Execute the where clause of the query.
        """
        if len(self.where_conditions) > 0:
            for condition in self.where_conditions:
                column = condition[1]
                operator = '==' if condition[0] == '=' else condition[0]
                value = f"'{condition[2]}'" if type(condition[2]) == str else condition[2]

                query = f"{column} {operator} {value}"
                self.df.query(query, inplace=True)
