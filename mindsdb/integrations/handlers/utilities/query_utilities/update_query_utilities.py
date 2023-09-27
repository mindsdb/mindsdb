from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.handlers.utilities.query_utilities.base_query_utilities import BaseQueryParser


class UPDATEQueryParser(BaseQueryParser):
    """
    Parses an UPDATE query into its component parts.

    Parameters
    ----------
    query : ast.Update
        Given SQL UPDATE query.
    """
    def __init__(self, query):
        super().__init__(query)
    
    def parse_query():
        pass

    def parse_set_clause():
        pass

