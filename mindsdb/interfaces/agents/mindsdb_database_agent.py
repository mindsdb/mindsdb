"""
Wrapper around MindsDB's executor and integration controller following the implementation of the original
langchain.sql_database.SQLDatabase class to partly replicate its behavior.
"""

import traceback
from typing import Any, Iterable, List, Optional

from mindsdb.utilities import log
from langchain_community.utilities import SQLDatabase
from mindsdb.interfaces.skills.sql_agent import SQLAgent

logger = log.getLogger(__name__)


def extract_essential(input: str) -> str:
    """Sometimes LLM include to input unnecessary data. We can't control stochastic nature of LLM, so we need to
    'clean' input somehow. LLM prompt contains instruction to enclose input between '$START$' and '$STOP$'.
    """
    if "$START$" in input:
        input = input.partition("$START$")[-1]
    if "$STOP$" in input:
        input = input.partition("$STOP$")[0]
    return input.strip(" ")


class MindsDBSQL(SQLDatabase):
    @staticmethod
    def custom_init(sql_agent: "SQLAgent") -> "MindsDBSQL":
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
        return "mindsdb"

    @property
    def table_info(self) -> str:
        """Information about all tables in the database."""
        return self._sql_agent.get_table_info()

    @property
    def kb_info(self) -> str:
        """Information about all knowledge bases in the database."""
        return self._sql_agent.get_knowledge_base_info()

    def get_usable_table_names(self) -> Iterable[str]:
        """Information about all tables in the database."""
        try:
            return self._sql_agent.get_usable_table_names()
        except Exception as e:
            # If there's an error accessing tables through the integration, handle it gracefully
            logger.warning(f"Error getting usable table names: {str(e)}")
            # Return an empty list instead of raising an exception
            return []

    def get_table_info_no_throw(self, table_names: Optional[List[str]] = None) -> str:
        for i in range(len(table_names)):
            table_names[i] = extract_essential(table_names[i])
        return self._sql_agent.get_table_info_safe(table_names)

    def run_no_throw(self, command: str, fetch: str = "all") -> str:
        """Execute a SQL command and return the result as a string.

        This method catches any exceptions and returns an error message instead of raising an exception.

        Args:
            command: The SQL command to execute
            fetch: Whether to fetch 'all' results or just 'one'

        Returns:
            A string representation of the result or an error message
        """
        command = extract_essential(command)

        try:
            # Log the query for debugging
            logger.info(f"Executing SQL query: {command}")

            return self._sql_agent.query(command)

        except Exception as e:
            logger.error(f"Error executing SQL command: {str(e)}\n{traceback.format_exc()}")
            # If this is a knowledge base query, provide a more helpful error message
            if "knowledge_base" in command.lower() or any(
                kb in command for kb in self._sql_agent.get_usable_knowledge_base_names()
            ):
                return f"Error executing knowledge base query: {str(e)}. Please check that the knowledge base exists and your query syntax is correct."
            return f"Error: {str(e)}"

    def get_usable_knowledge_base_names(self) -> List[str]:
        """Get a list of usable knowledge base names.

        Returns:
            A list of knowledge base names that can be used in queries
        """
        try:
            return self._sql_agent.get_usable_knowledge_base_names()
        except Exception as e:
            logger.error(f"Error getting usable knowledge base names: {str(e)}")
            return []

    def check_knowledge_base_permission(self, name):
        """Get a list of usable knowledge base names.

        Returns:
            A list of knowledge base names that can be used in queries
        """

        return self._sql_agent.check_knowledge_base_permission(name)
