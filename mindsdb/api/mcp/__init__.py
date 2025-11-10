from textwrap import dedent
from typing import Any
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log

logger = log.getLogger(__name__)


@dataclass
class AppContext:
    db: Any


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage application lifecycle with type-safe context"""
    # Initialize on startup
    db.init()
    try:
        yield AppContext(db=db)
    finally:
        # TODO: We need better way to handle this in storage/db.py
        pass


# Configure server with lifespan
mcp = FastMCP(
    "MindsDB",
    lifespan=app_lifespan,
    dependencies=["mindsdb"],  # Add any additional dependencies
)


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
        logger.exception("Error processing query:")
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
        logger.exception("Error while retrieving list of databases")
        return {
            "type": "error",
            "error_code": 0,
            "error_message": str(e),
        }


def _get_status(request: Request) -> JSONResponse:
    """
    Status endpoint that returns basic server information.
    This endpoint can be used by the frontend to check if the MCP server is running.
    """

    status_info = {
        "status": "ok",
        "service": "mindsdb-mcp",
    }

    return JSONResponse(status_info)


def get_mcp_app():
    app = mcp.sse_app()
    app.add_route("/status", _get_status, methods=["GET"])
    return app
