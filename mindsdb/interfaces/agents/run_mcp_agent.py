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
            print("Streaming response:")
            async for chunk in agent_wrapper.acompletion_stream(messages):
                content = chunk["choices"][0]["delta"].get("content", "")
                if content:
                    print(content, end="", flush=True)
            print("\n")
        else:
            print("Getting response...")
            response = await agent_wrapper.acompletion(messages)
            print(response["choices"][0]["message"]["content"])
    except Exception as e:
        logger.error(f"Error during agent conversation: {str(e)}")
        print(f"Error: {str(e)}")


async def execute_direct_query(query):
    """Execute a direct SQL query using MCP"""
    print(f"Executing direct SQL query: {query}")

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
            print("Available tools:", [tool.name for tool in tools_response.tools])

            # Find query tool
            query_tool = None
            for tool in tools_response.tools:
                if tool.name == "query":
                    query_tool = tool
                    break

            if not query_tool:
                print("Error: No 'query' tool found")
                return

            # Execute query
            result = await session.call_tool("query", {"query": query})
            print("Result:", result.content)
        except Exception as e:
            print(f"Error: {str(e)}")
            print("\nMake sure the MindsDB server is running with MCP enabled:")
            print("python -m mindsdb --api=mysql,mcp,http")


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
        print(f"Creating MCP client agent for '{args.agent}' in project '{args.project}'")
        print(f"Connecting to MCP server at {args.host}:{args.port}")
        print("\n(Make sure MindsDB server is running with MCP enabled: python -m mindsdb --api=mysql,mcp,http)")

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
            print("\nEntering interactive mode. Type 'exit' to quit.")
            print("\nAvailable commands:")
            print("  exit, quit - Exit the program")
            print("  clear - Clear conversation history")
            print("  sql: <query> - Execute a direct SQL query via MCP")

            messages = []

            while True:
                user_input = input("\nYou: ")

                # Check for special commands
                if user_input.lower() in ["exit", "quit"]:
                    break
                elif user_input.lower() == "clear":
                    messages = []
                    print("Conversation history cleared")
                    continue
                elif user_input.lower().startswith("sql:"):
                    # Direct SQL execution using the agent's session
                    sql_query = user_input[4:].strip()
                    print(f"Executing SQL: {sql_query}")
                    try:
                        # Use the tool from the agent
                        if hasattr(agent_wrapper.agent, "session") and agent_wrapper.agent.session:
                            result = await agent_wrapper.agent.session.call_tool("query", {"query": sql_query})
                            print("Result:", result.content)
                        else:
                            print("Error: No active MCP session")
                    except Exception as e:
                        print(f"SQL Error: {str(e)}")
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
        await agent_wrapper.cleanup()

    except Exception as e:
        logger.error(f"Error running MCP agent: {str(e)}")
        print(f"Error: {str(e)}")
        print("\nMake sure the MindsDB server is running with MCP enabled:")
        print("python -m mindsdb --api=mysql,mcp,http")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
