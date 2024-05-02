"""
    Wrapper around MindsDB's executor and integration controller following the implementation of the original
    langchain.sql_database.SQLDatabase class to partly replicate its behavior.
"""
from typing import Any, Iterable, List, Optional

from langchain.sql_database import SQLDatabase

from mindsdb.interfaces.skills.skill_tool import skill_tool
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MindsDBSQL(SQLDatabase):
    """ Can't modify signature, as LangChain does a Pydantic check."""
    def __init__(
        self,
        engine=None,
        database: Optional[str] = 'mindsdb',
        metadata: Optional[Any] = None,
        ignore_tables: Optional[List[str]] = None,
        include_tables: Optional[List[str]] = None,
        sample_rows_in_table_info: int = 3,
        schema: Optional[str] = None,
        indexes_in_table_info: bool = False,
        custom_table_info: Optional[dict] = None,
        view_support: Optional[bool] = True,
    ):
        # Some args above are not used in this class, but are kept for compatibility

        self._sql_agent = skill_tool.get_sql_agent(
            database,
            include_tables,
            ignore_tables,
            sample_rows_in_table_info
        )

    @property
    def dialect(self) -> str:
        return 'mindsdb'

    def get_usable_table_names(self) -> Iterable[str]:
        return self._sql_agent.get_usable_table_names()

    def get_table_info_no_throw(self, table_names: Optional[List[str]] = None) -> str:
        return self._sql_agent.get_table_info_safe(table_names)

    def run_no_throw(self, command: str, fetch: str = "all") -> str:
        return self._sql_agent.query_safe(command)