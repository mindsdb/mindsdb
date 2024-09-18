from typing import List

from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.tools import ListSQLDatabaseTool, InfoSQLDatabaseTool, QuerySQLDataBaseTool
from langchain_core.tools import BaseTool

from mindsdb.interfaces.skills.custom.text2sql.mindsdb_sql_tool import MindsDBSQLParserTool


class MindsDBSQLToolkit(SQLDatabaseToolkit):

    def get_tools(self, prefix='') -> List[BaseTool]:
        """Get the tools in the toolkit."""
        list_sql_database_tool = ListSQLDatabaseTool(name=f'sql_db_list_tables{prefix}', db=self.db)

        info_sql_database_tool_description = (
            "Input: A comma-separated list of tables. Output: Schema and sample rows for those tables. "
            f"Ensure tables exist by calling {list_sql_database_tool.name} first. "
            "Use this tool to investigate table schemas for needed columns. "
            "Get sample data with 'SELECT * FROM table LIMIT 3' before answering questions. "
            "Example Input: table1, table2, table3"
        )
        info_sql_database_tool = InfoSQLDatabaseTool(
            name=f'sql_db_schema{prefix}',
            db=self.db, description=info_sql_database_tool_description
        )

        query_sql_database_tool_description = (
            "Input: A detailed SQL query. Output: Database result or error message. "
            "For errors, rewrite and retry the query. For 'Unknown column' errors, use "
            f"{info_sql_database_tool.name} to check table fields. "
            "SQL best practices: "
            "- Return results in Markdown tables. "
            "- Limit to 10 results unless specified otherwise. "
            "- Run COUNT(*) first; inform user of total and offer to provide more. "
            "- Query only necessary columns, not all. "
            "- Use only existing column names from correct tables. "
            "- Use CURRENT_DATE for 'today' in queries. "
            "- Properly cast/convert date columns for comparisons. "
            "- Use database-specific syntax for date intervals. "
            "For PostgreSQL: Use NOW() for current date/time, cast dates with column_name::DATE, "
            "and use 'SELECT NOW() + INTERVAL '5 DAY'' for date arithmetic."
        )
        query_sql_database_tool = QuerySQLDataBaseTool(
            name=f'sql_db_query{prefix}',
            db=self.db, description=query_sql_database_tool_description
        )

        mindsdb_sql_parser_tool_description = (
            "Use this tool to ensure a SQL query passes the MindsDB SQL parser. "
            "If the query is not correct, it will be corrected and returned. Use the new query. "
            "If the query can't be corrected, an error is returned. In this case, rewrite and retry. "
            "If the query is correct, it will be parsed and returned. "
            f"ALWAYS run this tool before executing a query with {query_sql_database_tool.name}. "
            "To answer questions with database data: "
            "1. Identify relevant tables. "
            "2. Investigate table schemas for needed columns. "
            "3. Construct a query to answer the question. "
            "4. Run the query and return results. "
            "Use SELECT DISTINCT for categorical columns in WHERE clauses."
        )
        mindsdb_sql_parser_tool = MindsDBSQLParserTool(
            name=f'mindsdb_sql_parser_tool{prefix}',
            description=mindsdb_sql_parser_tool_description
        )

        return [
            query_sql_database_tool,
            info_sql_database_tool,
            list_sql_database_tool,
            mindsdb_sql_parser_tool,
        ]
