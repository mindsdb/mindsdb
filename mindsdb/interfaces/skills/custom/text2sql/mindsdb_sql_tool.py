from typing import Type
import re

from langchain.tools import BaseTool
from mindsdb_sql import parse_sql
from pydantic import BaseModel, Field


class _MindsDBSQLParserToolInput(BaseModel):
    tool_input: str = Field("", description="A SQL query to validate.")


class MindsDBSQLParserTool(BaseTool):
    name: str = "mindsdb_sql_parser_tool"
    description: str = "Parse a SQL query to check it is valid MindsDB SQL."
    args_schema: Type[BaseModel] = _MindsDBSQLParserToolInput

    def _clean_query(self, query: str) -> str:
        # Sometimes LLM can input markdown into query tools.
        cmd = re.sub(r'```(sql)?', '', query)
        return cmd

    def _run(self, query: str):
        """Validate the SQL query."""
        clean_query = self._clean_query(query)
        try:
            ast_query = parse_sql(clean_query, dialect='mindsdb')
            return "".join(f"valid query: {ast_query.to_string()}")
        except Exception as e:
            return "".join(f"invalid query, with error: {e}")
