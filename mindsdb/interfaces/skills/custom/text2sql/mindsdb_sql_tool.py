from typing import Type

from langchain.tools import BaseTool
from mindsdb_sql import parse_sql
from pydantic import BaseModel, Field


class _MindsDBSQLParserToolInput(BaseModel):
    tool_input: str = Field("", description="A SQL query to validate.")


class MindsDBSQLParserTool(BaseTool):
    name: str = "mindsdb_sql_parser_tool"
    description: str = "Parse a SQL query to check it is valid MindsDB SQL."
    args_schema: Type[BaseModel] = _MindsDBSQLParserToolInput

    def _run(self, query: str):
        """Validate the SQL query."""
        try:
            ast_query = parse_sql(query, dialect='mindsdb')
            return "".join(f"valid query: {ast_query.to_string()}")
        except Exception as e:
            return "".join(f"invalid query, with error: {e}")
