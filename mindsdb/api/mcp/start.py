import uvicorn
import anyio

from mindsdb.api.mcp import get_mcp_app
from mindsdb.utilities import log
from mindsdb.utilities.config import Config

logger = log.getLogger(__name__)


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
