from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Optional, Dict, Any

from mcp.server.fastmcp import FastMCP
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db

logger = log.getLogger(__name__)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[Dict]:
    """Manage application lifecycle"""
    try:
        yield {}
    finally:
        pass


# Configure server with lifespan
mcp = FastMCP(
    "MindsDB",
    lifespan=app_lifespan,
    dependencies=["mindsdb"]  # Add any additional dependencies
)


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
    if not isinstance(query, str):
        raise ValueError('Query must be a string')

    if context is None:
        context = {}

    logger.debug(f'Incoming MCP query: {query}')

    mysql_proxy = FakeMysqlProxy()
    mysql_proxy.set_context(context)

    try:
        result = mysql_proxy.process_query(query)

        if result.type == SQL_RESPONSE_TYPE.OK:
            return {"type": SQL_RESPONSE_TYPE.OK}

        elif result.type == SQL_RESPONSE_TYPE.TABLE:
            return {
                "type": SQL_RESPONSE_TYPE.TABLE,
                "data": result.data.to_lists(json_types=True),
                "column_names": [
                    x["alias"] or x["name"] if "alias" in x else x["name"]
                    for x in result.columns
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
    listing_query = "SHOW DATABASES"
    mysql_proxy = FakeMysqlProxy()

    try:
        result = mysql_proxy.process_query(listing_query)
        if result.type == SQL_RESPONSE_TYPE.ERROR:
            return {
                "type": "error",
                "error_code": result.error_code,
                "error_message": result.error_message,
            }

        elif result.type == SQL_RESPONSE_TYPE.OK:
            return {"type": "ok"}

        elif result.type == SQL_RESPONSE_TYPE.TABLE:
            data = result.data.to_lists(json_types=True)
            return data

    except Exception as e:
        return {
            "type": "error",
            "error_code": 0,
            "error_message": str(e),
        }


def start(*args, **kwargs):
    """Start the MCP server
    
    Args:
        host (str): Host to bind to
        port (int): Port to listen on
    """
    db.init()
    
    config = Config()
    port = int(config['api']['mcp'].get('port', 47337))
    host = config['api']['mcp'].get('host', '127.0.0.1')
    
    logger.info(f"Starting MCP server on {host}:{port}")
    mcp.settings.host = host
    mcp.settings.port = port
    
    try:
        mcp.run(transport="sse")  # Use SSE transport instead of stdio
    except Exception as e:
        logger.error(f"Error starting MCP server: {str(e)}")
        raise


if __name__ == "__main__":
    start()
