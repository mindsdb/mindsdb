from typing import Type
import re
from mindsdb_sql_parser import parse_sql
from pydantic import BaseModel, Field
from langchain_core.tools import BaseTool


from mindsdb.interfaces.agents.mindsdb_database_agent import extract_essential


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

    def _query_options(self, query):
        yield query
        if '\\_' in query:
            yield query.replace('\\_', '_')

    def _run(self, query: str):
        """Validate the SQL query."""
        query = extract_essential(query)
        clean_query = self._clean_query(query)
        for query in self._query_options(clean_query):
            try:
                ast_query = parse_sql(query)
                return "".join(f"valid query: {ast_query.to_string()}")
            except Exception as e:
                error = "".join(f"invalid query, with error: {e}")
                continue
        return error
