from typing import List
from textwrap import dedent

from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.tools import ListSQLDatabaseTool, InfoSQLDatabaseTool, QuerySQLDataBaseTool
from langchain_core.tools import BaseTool

from mindsdb.interfaces.skills.custom.text2sql.mindsdb_sql_tool import MindsDBSQLParserTool


class MindsDBSQLToolkit(SQLDatabaseToolkit):

    def get_tools(self, prefix='') -> List[BaseTool]:
        """Get the tools in the toolkit."""
        list_sql_database_tool = ListSQLDatabaseTool(
            name=f'sql_db_list_tables{prefix}',
            db=self.db,
            description=(
                "Input is an empty string, output is a comma-separated list of tables in the database. "
                "Each table name in the list may be in one of two formats: database_name.table_name or "
                "database_name.schema_name.table_name."
            )
        )

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

        query_sql_database_tool_description = dedent(f"""\
            Input: A detailed SQL query.
            Output: Database result or error message. For errors, rewrite and retry the query. For 'Unknown column' errors, use '{info_sql_database_tool.name}' to check table fields.
            This system is a highly intelligent and reliable PostgreSQL SQL skill designed to work with databases.
            Follow these instructions with utmost precision:
            1. Query Output Format:
               - Always return results in well-formatted **Markdown tables**.
               - Ensure clarity and proper structure for easy readability.
            2. Sample Data:
               - Before answering a question, if you don't have sample data about a table, **always** get sample data using `SELECT * FROM table LIMIT 3` from the tables you believe are relevant to formulating your answers.
            3. Categorical Data:
               - Whenever working with a column where values seem categorical, especially when filtering with `WHERE col = 'value'`, `WHERE col IN (list of values)`, or `WHERE col NOT IN (list of values)`, **always** retrieve the distinct values first.
               - Before writing your main query, always run `SELECT DISTINCT col` to fetch a list of unique values from that column. This step is mandatory to ensure accurate queries and responses.
            4. Result Limiting and Counting:
               - Unless instructed otherwise by the user, always run a count on the final query first using `SELECT COUNT(*)`.
               - If the count is greater than 10, limit the query to return only 10 results initially.
               - **Always** inform the user of the total number of results available and specify that you are providing the first 10 results.
               - Let the user know they can request additional results and/or specify how they would like the results ordered or grouped.
            5. Date Handling:
               - **Always** use PostgreSQL-compatible `CURRENT_DATE` or `NOW()` functions when working with dates—never assume or guess the current date.
               - For any date-related comparisons in the query, *always* ensure that your query casts the column being compared using `column_name::DATE [operator] ..`
               - Do not compare date values without casting columns to date.
               - For date interval operations, use Interval units as keywords. You can use keywords to specify units like days, hours, months, years, etc., directly without quotes. Examples:
                 SELECT NOW() + INTERVAL 5 DAY;
                 SELECT NOW() - INTERVAL 3 HOUR;
                 SELECT NOW() + INTERVAL 2 MONTH + INTERVAL 3 DAY;
                 SELECT NOW() - INTERVAL 1 YEAR;
            6. Query Best Practices:
               - Always send only one query at a time.
               - Query only necessary columns, not all.
               - Use only existing column names from correct tables.
               - Use database-specific syntax for date operations.
            7. Error Handling:
               - For errors, rewrite and retry the query.
               - For 'Unknown column' errors, check table fields using info_sql_database_tool.
            Adhere to these guidelines for all queries and responses. Ask for clarification if needed.
        """)

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
