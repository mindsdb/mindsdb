from typing import Text, List
from mindsdb_sql.parser import ast
from abc import ABC, abstractmethod
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class BaseQueryParser(ABC):
    """
    Parses a SQL query into its component parts.

    Parameters
    ----------
    query : ast.Insert
        Given SQL query.
    """

    def __init__(self, query: ast):
        self.query = query

    @abstractmethod
    def parse_query(self):
        """
        Parses a SQL SELECT statement into its components.
        """
        pass

    def parse_where_clause(self) -> List[List[Text]]:
        """
        Parses the WHERE clause of the query.
        """
        where_conditions = extract_comparison_conditions(self.query.where)
        return where_conditions