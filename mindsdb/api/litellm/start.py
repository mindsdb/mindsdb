import asyncio
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.agents.litellm_server import run_server, run_server_async

logger = log.getLogger(__name__)


async def start_async(verbose=False):
    """Start the LiteLLM server

    Args:
        verbose (bool): Whether to enable verbose logging
    """
    config = Config()

    # Get agent name from command line args
    agent_name = config.cmd_args.agent
    if not agent_name:
        logger.error("Agent name is required for LiteLLM server. Use --agent parameter.")
        return 1

    # Get project name or use default
    project_name = config.cmd_args.project or "mindsdb"

    # Get MCP server connection details
    mcp_host = config.get('api', {}).get('mcp', {}).get('host', '127.0.0.1')
    mcp_port = int(config.get('api', {}).get('mcp', {}).get('port', 47337))

    # Get LiteLLM server settings
    litellm_host = config.get('api', {}).get('litellm', {}).get('host', '0.0.0.0')
    litellm_port = int(config.get('api', {}).get('litellm', {}).get('port', 8000))

    logger.info(f"Starting LiteLLM server for agent '{agent_name}' in project '{project_name}'")
    logger.info(f"Connecting to MCP server at {mcp_host}:{mcp_port}")
    logger.info(f"Binding to {litellm_host}:{litellm_port}")

    return await run_server_async(
        agent_name=agent_name,
        project_name=project_name,
        mcp_host=mcp_host,
        mcp_port=mcp_port,
        host=litellm_host,
        port=litellm_port
    )


def start(verbose=False):
    """Start the LiteLLM server (synchronous wrapper)

    Args:
        verbose (bool): Whether to enable verbose logging
    """
    from mindsdb.interfaces.storage import db
    db.init()

    # Run the async function in the event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(start_async(verbose))

    if result == 0:
        # Run the server
        config = Config()
        agent_name = config.cmd_args.agent
        project_name = config.cmd_args.project or "mindsdb"
        mcp_host = config.get('api', {}).get('mcp', {}).get('host', '127.0.0.1')
        mcp_port = int(config.get('api', {}).get('mcp', {}).get('port', 47337))
        litellm_host = config.get('api', {}).get('litellm', {}).get('host', '0.0.0.0')
        litellm_port = int(config.get('api', {}).get('litellm', {}).get('port', 8000))

        return run_server(
            agent_name=agent_name,
            project_name=project_name,
            mcp_host=mcp_host,
            mcp_port=mcp_port,
            host=litellm_host,
            port=litellm_port
        )
    else:
        logger.error("LiteLLM server initialization failed")
        return result
