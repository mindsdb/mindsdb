from typing import Any
from textwrap import dedent

import uvicorn
import anyio

from mindsdb.api.mcp import get_mcp_app
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE
from mindsdb.utilities import log
from mindsdb.utilities.config import Config

logger = log.getLogger(__name__)

# MCP Queries
LISTING_QUERY = "SHOW DATABASES"


query_tool_description = dedent("""\
    Executes a SQL query against MindsDB.

    A database must be specified either in the `context` parameter or directly in the query string (e.g., `SELECT * FROM my_database.my_table`). Queries like `SELECT * FROM my_table` will fail without a `context`.

    Args:
        query (str): The SQL query to execute.
        context (dict, optional): The default database context. For example, `{"db": "my_postgres"}`.

    Returns:
        A dictionary describing the result.
        - For a successful query with no data to return (e.g., an `UPDATE` statement), the response is `{"type": "ok"}`.
        - If the query returns tabular data, the response is a dictionary containing `data` (a list of rows) and `column_names` (a list of column names). For example: `{"type": "table", "data": [[1, "a"], [2, "b"]], "column_names": ["column_a", "column_b"]}`.
        - In case of an error, a response is `{"type": "error", "error_message": "the error message"}`.
""")


@mcp.tool(name="query", description=query_tool_description)
def query(query: str, context: dict | None = None) -> dict[str, Any]:
    """Execute a SQL query against MindsDB

    Args:
        query: The SQL query to execute
        context: Optional context parameters for the query

    Returns:
        Dict containing the query results or error information
    """

    if context is None:
        context = {}

    logger.debug(f"Incoming MCP query: {query}")

    mysql_proxy = FakeMysqlProxy()
    mysql_proxy.set_context(context)

    try:
        result = mysql_proxy.process_query(query)

        if result.type == SQL_RESPONSE_TYPE.OK:
            return {"type": SQL_RESPONSE_TYPE.OK}

        if result.type == SQL_RESPONSE_TYPE.TABLE:
            return {
                "type": SQL_RESPONSE_TYPE.TABLE,
                "data": result.result_set.to_lists(json_types=True),
                "column_names": [column.alias or column.name for column in result.result_set.columns],
            }
        else:
            return {"type": SQL_RESPONSE_TYPE.ERROR, "error_code": 0, "error_message": "Unknown response type"}

    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        return {"type": SQL_RESPONSE_TYPE.ERROR, "error_code": 0, "error_message": str(e)}


list_databases_tool_description = (
    "Returns a list of all database connections currently available in MindsDB. "
    + "The tool takes no parameters and responds with a list of database names, "
    + 'for example: ["my_postgres", "my_mysql", "test_db"].'
)


@mcp.tool(name="list_databases", description=list_databases_tool_description)
def list_databases() -> list[str]:
    """
    List all databases in MindsDB

    Returns:
       list[str]: list of databases
    """

    mysql_proxy = FakeMysqlProxy()

    try:
        result = mysql_proxy.process_query(LISTING_QUERY)
        if result.type == SQL_RESPONSE_TYPE.ERROR:
            return {
                "type": "error",
                "error_code": result.error_code,
                "error_message": result.error_message,
            }

        elif result.type == SQL_RESPONSE_TYPE.OK:
            return {"type": "ok"}

        elif result.type == SQL_RESPONSE_TYPE.TABLE:
            data = result.result_set.to_lists(json_types=True)
            data = [val[0] for val in data]
            return data

    except Exception as e:
        return {
            "type": "error",
            "error_code": 0,
            "error_message": str(e),
        }





async def run_sse_async(app) -> None:
    """Run the server using SSE transport."""

    config = uvicorn.Config(
        app,
        host=app.host,
        port=app.port,
        log_level=None,
        log_config=None,
    )
    server = uvicorn.Server(config)
    await server.serve()


def start(*args, **kwargs):
    """Start the MCP server
    Args:
        host (str): Host to bind to
        port (int): Port to listen on
    """
    config = Config()
    port = int(config["api"].get("mcp", {}).get("port", 47337))
    host = config["api"].get("mcp", {}).get("host", "127.0.0.1")

    logger.info(f"Starting MCP server on {host}:{port}")

    try:
        anyio.run(run_sse_async(get_mcp_app(host, port)))
    except Exception as e:
        logger.error(f"Error starting MCP server: {str(e)}")
        raise


if __name__ == "__main__":
    start()
