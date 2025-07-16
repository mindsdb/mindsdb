import json
import asyncio
from typing import Dict, List, Any, Iterator, ClassVar
from contextlib import AsyncExitStack

import pandas as pd
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from mindsdb.utilities import log
from mindsdb.interfaces.agents.langchain_agent import LangchainAgent
from mindsdb.interfaces.storage import db
from langchain_core.tools import BaseTool

logger = log.getLogger(__name__)


class MCPQueryTool(BaseTool):
    """Tool that executes queries via MCP server"""

    name: ClassVar[str] = "mcp_query"
    description: ClassVar[str] = "Execute SQL queries against the MindsDB server via MCP protocol"

    def __init__(self, session: ClientSession):
        super().__init__()
        self.session = session

    async def _arun(self, query: str) -> str:
        """Execute a query via MCP asynchronously"""
        try:
            logger.info(f"Executing MCP query: {query}")
            # Find the appropriate tool for SQL queries
            tools_response = await self.session.list_tools()
            query_tool = None

            for tool in tools_response.tools:
                if tool.name == "query":
                    query_tool = tool
                    break

            if not query_tool:
                return "Error: No 'query' tool found in the MCP server"

            # Call the query tool
            result = await self.session.call_tool("query", {"query": query})

            # Process the results
            if isinstance(result.content, dict) and "data" in result.content and "column_names" in result.content:
                # Create a DataFrame from the results
                df = pd.DataFrame(result.content["data"], columns=result.content["column_names"])
                return df.to_string()

            # Return raw result for other types
            return f"Query executed successfully: {json.dumps(result.content)}"

        except Exception as e:
            logger.error(f"Error executing MCP query: {str(e)}")
            return f"Error executing query: {str(e)}"

    def _run(self, query: str) -> str:
        """Synchronous wrapper for async query function"""
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._arun(query))


# todo move instantiation to agent controller
class MCPLangchainAgent(LangchainAgent):
    """Extension of LangchainAgent that delegates to MCP server"""

    def __init__(
        self,
        agent: db.Agents,
        model: dict = None,
        llm_params: dict = None,
        mcp_host: str = "127.0.0.1",
        mcp_port: int = 47337,
    ):
        super().__init__(agent, model, llm_params)
        self.mcp_host = mcp_host
        self.mcp_port = mcp_port
        self.exit_stack = AsyncExitStack()
        self.session = None
        self.stdio = None
        self.write = None

    async def connect_to_mcp(self):
        """Connect to the MCP server using stdio transport"""
        if self.session is None:
            logger.info(f"Connecting to MCP server at {self.mcp_host}:{self.mcp_port}")
            try:
                # For connecting to an already running MCP server
                # Set up server parameters to connect to existing process
                server_params = StdioServerParameters(
                    command="python",
                    args=["-m", "mindsdb", "--api=mcp"],
                    env={"MCP_HOST": self.mcp_host, "MCP_PORT": str(self.mcp_port)},
                )

                logger.info(f"Connecting to MCP server at {self.mcp_host}:{self.mcp_port}")

                # Connect to the server
                stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
                self.stdio, self.write = stdio_transport
                self.session = await self.exit_stack.enter_async_context(ClientSession(self.stdio, self.write))

                await self.session.initialize()

                # Test the connection by listing tools
                tools_response = await self.session.list_tools()
                logger.info(
                    f"Successfully connected to MCP server. Available tools: {[tool.name for tool in tools_response.tools]}"
                )

            except Exception as e:
                logger.error(f"Failed to connect to MCP server: {str(e)}")
                raise ConnectionError(f"Failed to connect to MCP server: {str(e)}")

    def _langchain_tools_from_skills(self, llm):
        """Override to add MCP query tool along with other tools"""
        # Get tools from parent implementation
        tools = super()._langchain_tools_from_skills(llm)

        # Initialize MCP connection
        try:
            # Using the event loop directly instead of asyncio.run()
            loop = asyncio.get_event_loop()
            if self.session is None:
                loop.run_until_complete(self.connect_to_mcp())

            # Add MCP query tool if session is established
            if self.session:
                tools.append(MCPQueryTool(self.session))
                logger.info("Added MCP query tool to agent tools")
        except Exception as e:
            logger.error(f"Failed to add MCP query tool: {str(e)}")

        return tools

    def get_completion(self, messages, stream: bool = False):
        """Override to ensure MCP connection is established before getting completion"""
        try:
            # Ensure connection to MCP is established
            if self.session is None:
                # Using the event loop directly instead of asyncio.run()
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.connect_to_mcp())
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {str(e)}")

        # Call parent implementation to get completion
        response = super().get_completion(messages, stream)

        # Ensure response is a string (not a DataFrame)
        if hasattr(response, "to_string"):  # It's a DataFrame
            return response.to_string()

        return response

    async def cleanup(self):
        """Clean up resources"""
        if self.exit_stack:
            await self.exit_stack.aclose()
            self.session = None
            self.stdio = None
            self.write = None


class LiteLLMAgentWrapper:
    """Wrapper for MCPLangchainAgent that provides LiteLLM-compatible interface"""

    def __init__(self, agent: MCPLangchainAgent):
        self.agent = agent

    async def acompletion(self, messages: List[Dict[str, str]], **kwargs) -> Dict[str, Any]:
        """Async completion interface compatible with LiteLLM"""
        # Convert messages to format expected by agent
        formatted_messages = [
            {
                "question": msg["content"] if msg["role"] == "user" else "",
                "answer": msg["content"] if msg["role"] == "assistant" else "",
            }
            for msg in messages
        ]

        # Get completion from agent
        response = self.agent.get_completion(formatted_messages)

        # Ensure response is a string
        if not isinstance(response, str):
            if hasattr(response, "to_string"):  # It's a DataFrame
                response = response.to_string()
            else:
                response = str(response)

        # Format response in LiteLLM expected format
        return {
            "choices": [{"message": {"role": "assistant", "content": response}}],
            "model": self.agent.args["model_name"],
            "object": "chat.completion",
        }

    async def acompletion_stream(self, messages: List[Dict[str, str]], **kwargs) -> Iterator[Dict[str, Any]]:
        """Async streaming completion interface compatible with LiteLLM"""
        # Convert messages to format expected by agent
        formatted_messages = [
            {
                "question": msg["content"] if msg["role"] == "user" else "",
                "answer": msg["content"] if msg["role"] == "assistant" else "",
            }
            for msg in messages
        ]

        # Stream completion from agent
        model_name = kwargs.get("model", self.agent.args.get("model_name", "mcp-agent"))
        try:
            # Handle synchronous generator from _get_completion_stream
            for chunk in self.agent._get_completion_stream(formatted_messages):
                content = chunk.get("output", "")
                if content and isinstance(content, str):
                    yield {
                        "choices": [{"delta": {"role": "assistant", "content": content}}],
                        "model": model_name,
                        "object": "chat.completion.chunk",
                    }
                # Allow async context switch
                await asyncio.sleep(0)
        except Exception as e:
            logger.error(f"Streaming error: {str(e)}")
            raise

    async def cleanup(self):
        """Clean up resources"""
        await self.agent.cleanup()


def create_mcp_agent(
    agent_name: str, project_name: str, mcp_host: str = "127.0.0.1", mcp_port: int = 47337
) -> LiteLLMAgentWrapper:
    """Create an MCP agent and wrap it for LiteLLM compatibility"""
    from mindsdb.interfaces.agents.agents_controller import AgentsController
    from mindsdb.interfaces.storage import db

    # Initialize database
    db.init()

    # Get the agent from database
    agent_controller = AgentsController()
    agent_db = agent_controller.get_agent(agent_name, project_name)

    if agent_db is None:
        raise ValueError(f"Agent {agent_name} not found in project {project_name}")

    # Get merged parameters (defaults + agent params)
    llm_params = agent_controller.get_agent_llm_params(agent_db.params)

    # Create MCP agent with merged parameters
    mcp_agent = MCPLangchainAgent(agent_db, llm_params=llm_params, mcp_host=mcp_host, mcp_port=mcp_port)

    # Wrap for LiteLLM compatibility
    return LiteLLMAgentWrapper(mcp_agent)
