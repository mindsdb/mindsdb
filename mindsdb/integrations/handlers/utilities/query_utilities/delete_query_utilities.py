from mindsdb.integrations.handlers.utilities.query_utilities.base_query_utilities import BaseQueryParser
from mindsdb.integrations.handlers.utilities.query_utilities.base_query_utilities import BaseQueryExecutor


class DELETEQueryParser(BaseQueryParser):
    """
    Parses a DELETE query into its component parts.

    Parameters
    ----------
    query : ast.Delete
        Given SQL DELETE query.
    """
    def __init__(self, query):
        super().__init__(query)
    
    def parse_query(self):
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
    """
    