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
    Execute a SQL query against MindsDB and return the result.

    Queries use MySQL syntax. Use fully qualified names (`database`.`table`) or set `context` to specify
    the default database. Use backticks (`) to quote identifiers that are reserved words or contain
    special characters.

    Returns one of:
    - `{"type": "ok"}` — for statements with no output (INSERT, UPDATE, etc.)
    - `{"type": "table", "column_names": [...], "data": [[...], ...]}` — for SELECT results
    - `{"type": "error", "error_message": "..."}` — on failure
""")


@mcp.tool(name="query", description=query_tool_description)
def query(
    query: Annotated[str, Field(description="SQL query to execute against MindsDB.")],
    context: Annotated[
        dict | None,
        Field(
            description=(
                'Default database context, e.g. {"db": "my_postgres"}. '
                "Required if the query does not use fully qualified table names."
            )
        ),
    ] = None,
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
