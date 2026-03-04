from textwrap import dedent
from typing import Annotated

from pydantic import Field

from mindsdb.api.mcp.mcp_instance import mcp
from mindsdb.api.mcp.types import ErrorResponse, QueryResponseAnswer, response_adapter
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import SQLAnswer
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log

logger = log.getLogger(__name__)


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
def query(
    query: Annotated[str, Field(description="query string")],
    context: Annotated[dict | None, Field(description="Optional context parameters for the query")] = None,
) -> QueryResponseAnswer:
    ctx.set_default()

    if context is None:
        context = {}

    logger.debug(f"Incoming MCP query: {query}")

    mysql_proxy = FakeMysqlProxy()
    mysql_proxy.set_context(context)

    try:
        result: SQLAnswer = mysql_proxy.process_query(query)
        query_response: dict = result.dump_http_response()
    except Exception as e:
        logger.exception("Error processing query:")
        return ErrorResponse(type="error", error_code=0, error_message=str(e))

    return response_adapter.validate_python(query_response)
