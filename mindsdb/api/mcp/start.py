import requests

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Optional, Dict, Any
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP
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
            data = result.data.to_lists(json_types=True)
            return data

    except Exception as e:
        return {
            "type": "error",
            "error_code": 0,
            "error_message": str(e),
        }


@mcp.tool()
def create_view(view_name: str, data_query: str):
    """
    Create a view in MindsDB

    Arguments:
        view_name: The name of the view to create
        data_query: The query to create the view from

    Returns:
        Dict containing the list of databases and their tables
    """

    url = "http://127.0.0.1:47334/api/projects/mindsdb/views"

    payload = {"view": {
            "name": view_name,
            "query": data_query
        }}
    headers = {"Content-Type": "application/json"}

    try:
        # Add timeout to prevent hanging
        response = requests.request("POST", url, json=payload, headers=headers, timeout=10)
        
        # Check status code and handle errors
        response.raise_for_status()
        
        # Return structured response
        return {
            "status": "success",
            "message": f"View {view_name} created successfully",
            "details": response.json() if response.text else None
        }
    except requests.exceptions.Timeout:
        return {
            "status": "error",
            "message": "Request timed out after 10 seconds. The server took too long to respond."
        }
    except requests.exceptions.ConnectionError:
        return {
            "status": "error",
            "message": f"Failed to connect to MindsDB server at {url}. Please check if the server is running."
        }
    except requests.exceptions.RequestException as e:
        return {
            "status": "error",
            "message": f"Error creating view: {str(e)}"
        }


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
        mcp.run(transport="sse")  # Use SSE transport instead of stdio
    except Exception as e:
        logger.error(f"Error starting MCP server: {str(e)}")
        raise


if __name__ == "__main__":
    start()
