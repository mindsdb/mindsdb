"""
    Wrapper around MindsDB's executor and integration controller following the implementation of the original
    langchain.sql_database.SQLDatabase class to partly replicate its behavior.
"""
from typing import Any, Iterable, List, Optional

from mindsdb.utilities import log
from langchain_community.utilities import SQLDatabase
from mindsdb.interfaces.skills.sql_agent import SQLAgent

logger = log.getLogger(__name__)


class MindsDBSQL(SQLDatabase):
    @staticmethod
    def custom_init(
        sql_agent: 'SQLAgent'
    ) -> 'MindsDBSQL':
        instance = MindsDBSQL()
        instance._sql_agent = sql_agent
        return instance

    """ Can't modify signature, as LangChain does a Pydantic check."""
    def __init__(
        self,
        engine: Optional[Any] = None,
        schema: Optional[str] = None,
        metadata: Optional[Any] = None,
        ignore_tables: Optional[List[str]] = None,
        include_tables: Optional[List[str]] = None,
        sample_rows_in_table_info: int = 3,
        indexes_in_table_info: bool = False,
        custom_table_info: Optional[dict] = None,
        view_support: bool = True,
        max_string_length: int = 300,
        lazy_table_reflection: bool = False,
    ):
        pass

    @property
    def dialect(self) -> str:
        return 'mindsdb'

    @property
    def table_info(self) -> str:
        """Information about all tables in the database."""
        return self._sql_agent.get_table_info()

    def get_usable_table_names(self) -> Iterable[str]:
        return self._sql_agent.get_usable_table_names()

    def get_table_info_no_throw(self, table_names: Optional[List[str]] = None) -> str:
        return self._sql_agent.get_table_info_safe(table_names)

    def run_no_throw(self, command: str, fetch: str = "all") -> str:
        return self._sql_agent.query_safe(command)
