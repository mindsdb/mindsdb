from mindsdb_sql_parser.ast import (
    Select,
    Star,

)
from typing import List
import pandas as pd

from mindsdb.integrations.utilities.sql_utils import FilterCondition


class KeywordSearchBase:
    """
    Base class for keyword search integrations.
    This class provides a common interface for keyword search functionality.
    """

    def __init__(self, *args, **kwargs):
        pass

    def dispatch_keyword_select(self, query: Select, conditions: List[FilterCondition] = None):
        """
        Dispatch select query to the appropriate method.
        """
        # parse key arguments
        table_name = query.from_table.parts[-1]
        # if targets are star, select all columns
        if isinstance(query.targets[0], Star):
            columns = [col["name"] for col in self.SCHEMA]
        else:
            columns = [col.parts[-1] for col in query.targets]

        if not self._is_columns_allowed(columns):
            raise Exception(
                f"Columns {columns} not allowed."
                f"Allowed columns are {[col['name'] for col in self.SCHEMA]}"
            )

        # check if columns are allowed
        if conditions is None:
            where_statement = query.where
            conditions = self.extract_conditions(where_statement)
        self._convert_metadata_filters(conditions)

        # get offset and limit
        offset = query.offset.value if query.offset is not None else None
        limit = query.limit.value if query.limit is not None else None

        # dispatch select
        return self.keyword_select(
            table_name,
            columns=columns,
            conditions=conditions,
            offset=offset,
            limit=limit,
        )

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
