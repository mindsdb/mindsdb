from mindsdb_sql.parser import ast
from typing import Text, List, Optional

from .exceptions import UnsupportedColumnException

from mindsdb.integrations.handlers.utilities.query_utilities.base_query_utilities import BaseQueryParser
from mindsdb.integrations.handlers.utilities.query_utilities.base_query_utilities import BaseQueryExecutor


class UPDATEQueryParser(BaseQueryParser):
    """
    Parses an UPDATE query into its component parts.

    Parameters
    ----------
    query : ast.Update
        Given SQL UPDATE query.
    supported_columns : List[Text], Optional
        List of columns supported by the table for updating.
    """
    def __init__(self, query: ast.Update, supported_columns: Optional[List[Text]] = None):
        super().__init__(query)
        self.supported_columns = supported_columns
    
    def parse_query(self):
        """
        Parses a SQL UPDATE statement into its components: the columns and values to update as a dictionary, and the WHERE conditions.
        """
        values_to_update = self.parse_set_clause()
        where_conditions = self.parse_where_clause()

        return values_to_update, where_conditions

    def parse_set_clause(self):
        """
        Parses the SET clause of the query and returns a dictionary of columns and values to update.
        """
        values = list(self.query.update_columns.items())

        values_to_update = {}
        for value in values:
            if self.supported_columns:
                if value[0] not in self.supported_columns:
                    raise UnsupportedColumnException(f"Unsupported column: {value[0]}")
            
            values_to_update[value[0]] = value[1].value

        return values_to_update


class UPDATEQueryExecutor(BaseQueryExecutor):
    """
    Executes an UPDATE query.

    Parameters
    ----------
    df : pd.DataFrame
        Given table.
    where_conditions : List[List[Text]]
        WHERE conditions of the query.

    NOTE: This class DOES NOT update the relevant records of the entity for you, it will simply return the records that need to be updated based on the WHERE conditions.
          
          This class expects all of the entities to be passed in as a DataFrane and filters out the relevant records based on the WHERE conditions.
          Because all of the records need to be extracted to be passed in as a DataFrame, this class is not very computationally efficient.
          Therefore, DO NOT use this class if the API/SDK that you are using supports updating records in bulk.
    """
from mindsdb_sql.parser import ast
from typing import Text, List, Optional

from .exceptions import UnsupportedColumnException

from mindsdb.integrations.handlers.utilities.query_utilities.base_query_utilities import BaseQueryParser
from mindsdb.integrations.handlers.utilities.query_utilities.base_query_utilities import BaseQueryExecutor


class UPDATEQueryParser(BaseQueryParser):
    """
    Parses an UPDATE query into its component parts.

    Parameters
    ----------
    query : ast.Update
        Given SQL UPDATE query.
    supported_columns : List[Text], Optional
        List of columns supported by the table for updating.
    """
    def __init__(self, query: ast.Update, supported_columns: Optional[List[Text]] = None):
        super().__init__(query)
        self.supported_columns = supported_columns
    
    def parse_query(self):
        """
        Parses a SQL UPDATE statement into its components: the columns and values to update as a dictionary, and the WHERE conditions.
        """
        values_to_update = self.parse_set_clause()
        where_conditions = self.parse_where_clause()

        return values_to_update, where_conditions

    def parse_set_clause(self):
        """
        Parses the SET clause of the query and returns a dictionary of columns and values to update.
        """
        values = list(self.query.update_columns.items())

        values_to_update = {}
        for value in values:
            if self.supported_columns:
                if value[0] not in self.supported_columns:
                    raise UnsupportedColumnException(f"Unsupported column: {value[0]}")
            
            values_to_update[value[0]] = value[1].value

        return values_to_update


class UPDATEQueryExecutor(BaseQueryExecutor):
    """
    Executes an UPDATE query.

    Parameters
    ----------
    df : pd.DataFrame
        Given table.
    where_conditions : List[List[Text]]
        WHERE conditions of the query.

    NOTE: This class DOES NOT update the relevant records of the entity for you, it will simply return the records that need to be updated based on the WHERE conditions.
          
          This class expects all of the entities to be passed in as a DataFrane and filters out the relevant records based on the WHERE conditions.
          Because all of the records need to be extracted to be passed in as a DataFrame, this class is not very computationally efficient.
          Therefore, DO NOT use this class if the API/SDK that you are using supports updating records in bulk.
    """
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

    def parse_query(self) -> Tuple[Dict[Text, Any], List[List[Text]]]:
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
