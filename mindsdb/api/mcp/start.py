import os
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Optional, Dict, Any
from dataclasses import dataclass

import uvicorn
import anyio
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db

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
    dependencies=["mindsdb"]  # Add any additional dependencies
)
# MCP Queries
LISTING_QUERY = "SHOW DATABASES"


@mcp.tool()
def query(query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Execute a SQL query against MindsDB

    Args:
        query: The SQL query to execute
        context: Optional context parameters for the query

    Returns:
        Dict containing the query results or error information
    """

    if context is None:
        context = {}

    logger.debug(f'Incoming MCP query: {query}')

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
                "column_names": [
                    column.alias or column.name
                    for column in result.result_set.columns
                ],
            }
        else:
            return {
                "type": SQL_RESPONSE_TYPE.ERROR,
                "error_code": 0,
                "error_message": "Unknown response type"
            }

    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        return {
            "type": SQL_RESPONSE_TYPE.ERROR,
            "error_code": 0,
            "error_message": str(e)
        }


@mcp.tool()
def list_databases() -> Dict[str, Any]:
    """
    List all databases in MindsDB along with their tables

    Returns:
        Dict containing the list of databases and their tables
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
            return data

    except Exception as e:
        return {
            "type": "error",
            "error_code": 0,
            "error_message": str(e),
        }


class CustomAuthMiddleware(BaseHTTPMiddleware):
    """Custom middleware to handle authentication basing on header 'Authorization'
    """
    async def dispatch(self, request: Request, call_next):
        mcp_access_token = os.environ.get('MINDSDB_MCP_ACCESS_TOKEN')
        if mcp_access_token is not None:
            auth_token = request.headers.get('Authorization', '').partition('Bearer ')[-1]
            if mcp_access_token != auth_token:
                return Response(status_code=401, content="Unauthorized", media_type="text/plain")

        response = await call_next(request)

        return response


async def run_sse_async() -> None:
    """Run the server using SSE transport."""
    starlette_app = mcp.sse_app()
    starlette_app.add_middleware(CustomAuthMiddleware)

    config = uvicorn.Config(
        starlette_app,
        host=mcp.settings.host,
        port=mcp.settings.port,
        log_level=mcp.settings.log_level.lower(),
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
    port = int(config['api'].get('mcp', {}).get('port', 47337))
    host = config['api'].get('mcp', {}).get('host', '127.0.0.1')

    logger.info(f"Starting MCP server on {host}:{port}")
    mcp.settings.host = host
    mcp.settings.port = port

    try:
        anyio.run(run_sse_async)
    except Exception as e:
        logger.error(f"Error starting MCP server: {str(e)}")
        raise


if __name__ == "__main__":
    start()
