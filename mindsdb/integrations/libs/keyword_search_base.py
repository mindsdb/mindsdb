from mindsdb_sql_parser.ast import Select
from typing import List
import pandas as pd

from mindsdb.integrations.utilities.sql_utils import FilterCondition, KeywordSearchArgs


class KeywordSearchBase:
    """
    Base class for keyword search integrations.
    This class provides a common interface for keyword search functionality.
    """

    def __init__(self, *args, **kwargs):
        pass

    def dispatch_keyword_select(
        self, query: Select, conditions: List[FilterCondition] = None, keyword_search_args: KeywordSearchArgs = None
    ):
        """Dispatches a keyword search select query to the appropriate method."""
        raise NotImplementedError()

    def keyword_select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Select data from table

        Args:
            table_name (str): table name
            columns (List[str]): columns to select
            conditions (List[FilterCondition]): conditions to select

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()
