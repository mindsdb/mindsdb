import sys
import argparse
import asyncio
from typing import List, Dict
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from mindsdb.utilities import log
from mindsdb.interfaces.agents.mcp_client_agent import create_mcp_agent

logger = log.getLogger(__name__)


async def run_conversation(agent_wrapper, messages: List[Dict[str, str]], stream: bool = False):
    """Run a conversation with the agent and print responses"""
    try:
        if stream:
            logger.info("Streaming response:")
            async for chunk in agent_wrapper.acompletion_stream(messages):
                content = chunk["choices"][0]["delta"].get("content", "")
                if content:
                    # We still need to print content for streaming display
                    # but we'll log it as debug as well
                    logger.debug(f"Stream content: {content}")
                    sys.stdout.write(content)
                    sys.stdout.flush()
            logger.debug("End of stream")
            sys.stdout.write("\n\n")
            sys.stdout.flush()
        else:
            logger.info("Getting response...")
            response = await agent_wrapper.acompletion(messages)
            content = response["choices"][0]["message"]["content"]
            logger.info(f"Response: {content}")
            # We still need to display the response to the user
            sys.stdout.write(f"{content}\n")
            sys.stdout.flush()
    except Exception as e:
        logger.error(f"Error during agent conversation: {str(e)}")


async def execute_direct_query(query):
    """Execute a direct SQL query using MCP"""
    logger.info(f"Executing direct SQL query: {query}")

    # Set up MCP client to connect to the running server
    async with AsyncExitStack() as stack:
        # Connect to MCP server
        server_params = StdioServerParameters(
            command="python",
            args=["-m", "mindsdb", "--api=mcp"],
            env=None
        )

        try:
            stdio_transport = await stack.enter_async_context(stdio_client(server_params))
            stdio, write = stdio_transport
            session = await stack.enter_async_context(ClientSession(stdio, write))

            await session.initialize()

            # List available tools
            tools_response = await session.list_tools()
            tool_names = [tool.name for tool in tools_response.tools]
            logger.info(f"Available tools: {tool_names}")

            # Find query tool
            query_tool = None
            for tool in tools_response.tools:
                if tool.name == "query":
                    query_tool = tool
                    break

            if not query_tool:
                logger.error("No 'query' tool found in MCP server")
                return

            # Execute query
            result = await session.call_tool("query", {"query": query})
            logger.info(f"Query result: {result.content}")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.info("Make sure the MindsDB server is running with MCP enabled: python -m mindsdb --api=mysql,mcp,http")


async def main():
    parser = argparse.ArgumentParser(description="Run an agent as an MCP client")
    parser.add_argument("--agent", type=str, help="Name of the agent to use")
    parser.add_argument("--project", type=str, default="mindsdb", help="Project containing the agent")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="MCP server host")
    parser.add_argument("--port", type=int, default=47337, help="MCP server port")
    parser.add_argument("--query", type=str, help="Query to send to the agent")
    parser.add_argument("--stream", action="store_true", help="Stream the response")
    parser.add_argument("--execute-direct", type=str, help="Execute a direct SQL query via MCP (for testing)")

    args = parser.parse_args()

    try:
        # Initialize database connection
        from mindsdb.interfaces.storage import db
        db.init()

        # Direct SQL execution mode (for testing MCP connection)
        if args.execute_direct:
            await execute_direct_query(args.execute_direct)
            return 0

        # Make sure agent name is provided
        if not args.agent:
            parser.error("the --agent argument is required unless --execute-direct is used")

        # Create the agent
        logger.info(f"Creating MCP client agent for '{args.agent}' in project '{args.project}'")
        logger.info(f"Connecting to MCP server at {args.host}:{args.port}")
        logger.info("Make sure MindsDB server is running with MCP enabled: python -m mindsdb --api=mysql,mcp,http")

        agent_wrapper = create_mcp_agent(
            agent_name=args.agent,
            project_name=args.project,
            mcp_host=args.host,
            mcp_port=args.port
        )

        # Run an example query if provided
        if args.query:
            messages = [{"role": "user", "content": args.query}]
            await run_conversation(agent_wrapper, messages, args.stream)
        else:
            # Interactive mode
            logger.info("Entering interactive mode. Type 'exit' to quit.")
            logger.info("Available commands: exit/quit, clear, sql:")

            # We still need to show these instructions to the user
            sys.stdout.write("\nEntering interactive mode. Type 'exit' to quit.\n")
            sys.stdout.write("\nAvailable commands:\n")
            sys.stdout.write("  exit, quit - Exit the program\n")
            sys.stdout.write("  clear - Clear conversation history\n")
            sys.stdout.write("  sql: <query> - Execute a direct SQL query via MCP\n")
            sys.stdout.flush()

            messages = []

            while True:
                # We need to keep input for user interaction
                user_input = input("\nYou: ")

                # Check for special commands
                if user_input.lower() in ["exit", "quit"]:
                    logger.info("Exiting interactive mode")
                    break
                elif user_input.lower() == "clear":
                    messages = []
                    logger.info("Conversation history cleared")
                    sys.stdout.write("Conversation history cleared\n")
                    sys.stdout.flush()
                    continue
                elif user_input.lower().startswith("sql:"):
                    # Direct SQL execution using the agent's session
                    sql_query = user_input[4:].strip()
                    logger.info(f"Executing SQL: {sql_query}")
                    try:
                        # Use the tool from the agent
                        if hasattr(agent_wrapper.agent, "session") and agent_wrapper.agent.session:
                            result = await agent_wrapper.agent.session.call_tool("query", {"query": sql_query})
                            logger.info(f"SQL result: {result.content}")
                            # We need to show the result to the user
                            sys.stdout.write(f"Result: {result.content}\n")
                            sys.stdout.flush()
                        else:
                            logger.error("No active MCP session")
                            sys.stdout.write("Error: No active MCP session\n")
                            sys.stdout.flush()
                    except Exception as e:
                        logger.error(f"SQL Error: {str(e)}")
                        sys.stdout.write(f"SQL Error: {str(e)}\n")
                        sys.stdout.flush()
                    continue

                messages.append({"role": "user", "content": user_input})
                await run_conversation(agent_wrapper, messages, args.stream)

                # Add assistant's response to the conversation history
                if not args.stream:
                    response = await agent_wrapper.acompletion(messages)
                    messages.append({
                        "role": "assistant",
                        "content": response["choices"][0]["message"]["content"]
                    })

        # Clean up resources
        logger.info("Cleaning up resources")
        await agent_wrapper.cleanup()

    except Exception as e:
        logger.error(f"Error running MCP agent: {str(e)}")
        logger.info("Make sure the MindsDB server is running with MCP enabled: python -m mindsdb --api=mysql,mcp,http")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
