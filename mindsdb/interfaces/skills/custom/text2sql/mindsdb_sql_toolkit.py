from typing import List

from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.tools import ListSQLDatabaseTool, InfoSQLDatabaseTool, QuerySQLDataBaseTool
from langchain_core.tools import BaseTool

from mindsdb.interfaces.skills.custom.text2sql.mindsdb_sql_tool import MindsDBSQLParserTool


class MindsDBSQLToolkit(SQLDatabaseToolkit):

    def get_tools(self) -> List[BaseTool]:
        # Return the tools that this toolkit provides as well as MindDB's SQL validator tool

        """Get the tools in the toolkit."""
        list_sql_database_tool = ListSQLDatabaseTool(db=self.db)
        info_sql_database_tool_description = (
            "Input to this tool is a comma-separated list of tables, output is the "
            "schema and sample rows for those tables. "
            "Be sure that the tables actually exist by calling "
            f"{list_sql_database_tool.name} first! "
            "Example Input: table1, table2, table3"
        )
        info_sql_database_tool = InfoSQLDatabaseTool(
            db=self.db, description=info_sql_database_tool_description
        )
        query_sql_database_tool_description = (
            "Input to this tool is a detailed and correct SQL query, output is a "
            "result from the database. If the query is not correct, an error message "
            "will be returned. If an error is returned, rewrite the query, check the "
            "query, and try again. If you encounter an issue with Unknown column "
            f"'xxxx' in 'field list', use {info_sql_database_tool.name} "
            "to query the correct table fields."
        )
        query_sql_database_tool = QuerySQLDataBaseTool(
            db=self.db, description=query_sql_database_tool_description
        )

        mindsdb_sql_parser_tool_description = (
            "Use this tool to ensure that a SQL query passes the MindsDB SQL parser."
            "If the query is not correct, it will be corrected and the new query will be returned. Use this new query."
            "If the query is not correct and cannot be corrected, an error will be returned."
            "In this case an error is returned, rewrite the query, check the query, and try again."
            "If query is correct, the query will be parsed and returned."
            "This tool should ALWAYS be run before executing a query with the tool  "
            f"{query_sql_database_tool.name}!"
            ""
        )

        mindsdb_sql_parser_tool = MindsDBSQLParserTool(
            description=mindsdb_sql_parser_tool_description
        )

        return [
            query_sql_database_tool,
            info_sql_database_tool,
            list_sql_database_tool,
            mindsdb_sql_parser_tool,
        ]
