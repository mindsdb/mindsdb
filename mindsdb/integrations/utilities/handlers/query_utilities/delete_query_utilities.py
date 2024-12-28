from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.handlers.query_utilities.base_query_utilities import BaseQueryParser
from mindsdb.integrations.utilities.handlers.query_utilities.base_query_utilities import BaseQueryExecutor


class DELETEQueryParser(BaseQueryParser):
    """
    Parses a DELETE query into its component parts.

    Parameters
    ----------
    query : ast.Delete
        Given SQL DELETE query.
    """
    def __init__(self, query: ast.Delete):
        super().__init__(query)

    def parse_query(self):
        """
        Parses a SQL DELETE statement into its components: WHERE.
        """
        where_conditions = self.parse_where_clause()

        return where_conditions


class DELETEQueryExecutor(BaseQueryExecutor):
    """
    Executes a DELETE query.

    Parameters
    ----------
    df : pd.DataFrame
        Given table.
    where_conditions : List[List[Text]]
        WHERE conditions of the query.

    NOTE: This class DOES NOT delete the relevant records of the entity for you, it will simply return the records that need to be deleted based on the WHERE conditions.

          This class expects all of the entities to be passed in as a DataFrane and filters out the relevant records based on the WHERE conditions.
          Because all of the records need to be extracted to be passed in as a DataFrame, this class is not very computationally efficient.
          Therefore, DO NOT use this class if the API/SDK that you are using supports deleting records in bulk.
    """
