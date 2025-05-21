#!/usr/bin/env python
"""
Integration test for the MindsDBAgent streaming functionality.
This script demonstrates how to use the stream method to get streaming responses from a MindsDB agent.

Prerequisites:
    - A running MindsDB server
    - An existing agent created in MindsDB (this script does NOT create the agent)
    - Example agent creation SQL:
        CREATE AGENT my_agent_1422
        USING
            provider='openai',
            model='gpt-4',
            database='mindsdb',
            include_tables=['postgresql_conn.home_rentals']

Usage:
    python -m tests.integration.a2a.test_agent_streaming_direct [options]

Options:
    --invoke          Test the streaming_invoke method instead of stream
    --agent=NAME      Specify the agent name (default: my_agent_1422)
    --project=NAME    Specify the project name (default: mindsdb)
    --host=HOST       Specify the MindsDB host (default: 127.0.0.1)
    --port=PORT       Specify the MindsDB port (default: 47334)
    --query=QUERY     Specify the test query (default: "What is the average rental price for a three bedroom?")
    --timeout=SECS    Specify request timeout in seconds (default: 300)
"""

import asyncio
import json
import argparse
import os
from mindsdb.api.a2a.agent import MindsDBAgent


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Test MindsDBAgent streaming functionality"
    )
    parser.add_argument(
        "--invoke", action="store_true", help="Test streaming_invoke method"
    )
    parser.add_argument("--agent", default="my_agent_1422", help="Agent name")
    parser.add_argument("--project", default="mindsdb", help="Project name")
    parser.add_argument("--host", default="127.0.0.1", help="MindsDB host")
    parser.add_argument("--port", default=47334, type=int, help="MindsDB port")
    parser.add_argument(
        "--query",
        default="What is the average rental price for a three bedroom?",
        help="Test query",
    )
    parser.add_argument(
        "--timeout", default=300, type=int, help="Request timeout in seconds"
    )

    # Check for environment variables first, then use command line args
    args = parser.parse_args()

    # Override with environment variables if present
    args.agent = os.environ.get("MINDSDB_TEST_AGENT", args.agent)
    args.project = os.environ.get("MINDSDB_TEST_PROJECT", args.project)
    args.host = os.environ.get("MINDSDB_TEST_HOST", args.host)
    args.port = int(os.environ.get("MINDSDB_TEST_PORT", args.port))
    args.query = os.environ.get("MINDSDB_TEST_QUERY", args.query)
    args.timeout = int(os.environ.get("MINDSDB_TEST_TIMEOUT", args.timeout))

    return args


async def test_stream(args):
    """Test the streaming functionality of MindsDBAgent."""
    print(f"Testing stream method with agent: {args.agent}")
    print(f"Connecting to {args.host}:{args.port}, project: {args.project}")

    # Create an agent instance
    agent = MindsDBAgent(
        agent_name=args.agent,
        project_name=args.project,
        host=args.host,
        port=args.port,
    )

    # Test query
    query = args.query
    session_id = "test_session"

    print(f"Sending query: {query}")
    print("Streaming response:")

    # Process streaming response
    async for chunk in agent.stream(query, session_id):
        # Print each chunk as it arrives
        print(f"Chunk: {json.dumps(chunk, indent=2)}")

        # Optional: Extract and print just the text content for easier reading
        if "parts" in chunk:
            for part in chunk["parts"]:
                if part.get("type") == "text":
                    print(f"Text: {part.get('text')}")
        elif "output" in chunk:
            print(f"Output: {chunk['output']}")

        # Check if this is the last chunk
        if chunk.get("is_task_complete", False) or chunk.get("type") == "end":
            print("Stream completed.")


def test_streaming_invoke(args):
    """Test the streaming_invoke method directly."""
    print(f"Testing streaming_invoke method with agent: {args.agent}")
    print(f"Connecting to {args.host}:{args.port}, project: {args.project}")

    # Create an agent instance
    agent = MindsDBAgent(
        agent_name=args.agent,
        project_name=args.project,
        host=args.host,
        port=args.port,
    )

    # Test messages
    messages = [{"question": args.query, "answer": None}]

    print(f"Sending messages: {json.dumps(messages)}")
    print("Streaming response:")

    # Process streaming response
    for chunk in agent.streaming_invoke(messages, timeout=args.timeout):
        # Print each chunk as it arrives
        print(f"Chunk: {json.dumps(chunk, indent=2)}")

        # Check for errors
        if "error" in chunk:
            print(f"Error encountered: {chunk['error']}")
            if chunk.get("is_task_complete", False):
                print("Stream completed with error.")
                return

        # Check for completion
        if chunk.get("is_task_complete", False) or chunk.get("type") == "end":
            print("Stream completed successfully.")


# Run the tests
if __name__ == "__main__":
    # Verify prerequisites
    print("IMPORTANT: This test requires an existing agent in MindsDB.")
    print("Make sure you have created the agent before running this test.")
    print("Example SQL to create an agent:")
    print(
        """
    CREATE AGENT my_agent_1422
    USING
        provider='openai',
        model='gpt-4',
        database='mindsdb',
        include_tables=['postgresql_conn.home_rentals']
    """
    )
    print("-" * 80)

    args = parse_args()

    if args.invoke:
        # Test streaming_invoke directly
        print("Testing streaming_invoke method:")
        test_streaming_invoke(args)
    else:
        # Test stream method by default
        print("Testing stream method:")
        asyncio.run(test_stream(args))
