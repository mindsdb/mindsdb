import pandas as pd
from typing import Text, List, Tuple, Any, Dict

from .exceptions import UnsupportedColumnException

from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

class UPDATEQueryParser():
    """
    Parses an UPDATE query into its component parts.

    Parameters
    ----------
    query : ast.Update
        Given SQL UPDATE query.
    table : Text
        Name of the table to query.
    columns : List[Text]
        List of columns in the table.
    """
    def __init__(self, query: ast.Update, table: Text, columns: List[Text]):
        self.query = query
        self.table = table
        self.columns = columns

    def parse_query(self) -> Tuple[List[Tuple[Text, Any]], List[List[Text]]]:
        """
        Parses a SQL UPDATE statement into its components: UPDATE, SET, WHERE.
        """
        set_clauses = self.parse_set_clause() #Columns and values to update
        where_conditions = self.parse_where_clause() #Conditional write

        return set_clauses, where_conditions
    
    
    def parse_set_clause(self) -> Dict[Text, Any]:
        column_value_pairs = list(self.query.update_columns.items())

        columns_to_update = {}
        for c_v_pair in column_value_pairs:
            columns_to_update[c_v_pair[0]] = c_v_pair[1]

        return columns_to_update


    def parse_where_clause(self) -> List[List[Text]]:
        """
        Parses the WHERE clause of the query.
        """
        where_conditions = extract_comparison_conditions(self.query.where)
        return where_conditions


class UPDATEQueryExecutor():
    """
    Executes a UPDATE query.
    """
    def __init__(self, df: pd.DataFrame, set_clauses: List[Tuple[Text, Any]], where_conditions: List[List[Text]]):
        """
        Initializes the UPDATEQueryExecutor,
        
        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to query.
        set_clauses : List[Tuple[Text, Any]]
            List of Tuples (Column, Value) to set.
        where_conditions : List[List[Text]]
            List of where conditions.
        """
        self.df = df
        self.set_clauses = set_clauses
        self.where_conditions = where_conditions

    def execute_query(self) -> pd.DataFrame:
        """
        Execute the query.
        """
        self.execute_where_clause()

        self.execute_set_clauses()

        return self.df
    
    def execute_set_clauses(self)  -> None:
        """
        Execute the set clause of the query.
        """
        for column, value in self.set_clauses.items():
            if not isinstance(column, str):
                raise ValueError("The column name should be a string")
            
            self.df[column] = value

    def execute_where_clause(self) -> None:
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
