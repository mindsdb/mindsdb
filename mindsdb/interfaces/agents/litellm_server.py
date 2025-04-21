import asyncio
import argparse
import json

from typing import List, Dict, Optional
from contextlib import AsyncExitStack

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from mindsdb.utilities import log
from mindsdb.interfaces.agents.mcp_client_agent import create_mcp_agent

logger = log.getLogger(__name__)

app = FastAPI(title="MindsDB MCP Agent LiteLLM API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store agent wrapper as a global variable
agent_wrapper = None
# MCP session for direct SQL queries
mcp_session = None
exit_stack = AsyncExitStack()


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    stream: bool = False
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None


class ChatCompletionChoice(BaseModel):
    index: int = 0
    message: Optional[Dict[str, str]] = None
    delta: Optional[Dict[str, str]] = None
    finish_reason: Optional[str] = "stop"


class ChatCompletionResponse(BaseModel):
    id: str = "mcp-agent-response"
    object: str = "chat.completion"
    created: int = 0
    model: str
    choices: List[ChatCompletionChoice]
    usage: Dict[str, int] = Field(default_factory=lambda: {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})


class DirectSQLRequest(BaseModel):
    query: str


@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest):
    global agent_wrapper

    if agent_wrapper is None:
        raise HTTPException(status_code=500, detail="Agent not initialized. Make sure MindsDB server is running with MCP enabled: python -m mindsdb --api=mysql,mcp,http")

    try:
        # Convert request to messages format
        messages = [
            {"role": msg.role, "content": msg.content}
            for msg in request.messages
        ]

        if request.stream:
            # Return a streaming response
            async def generate():
                try:
                    async for chunk in agent_wrapper.acompletion_stream(messages, model=request.model):
                        yield f"data: {json.dumps(chunk)}\n\n"
                    yield "data: [DONE]\n\n"
                except Exception as e:
                    logger.error(f"Streaming error: {str(e)}")
                    yield "data: {{'error': 'Streaming failed due to an internal error.'}}\n\n"
            return StreamingResponse(generate(), media_type="text/event-stream")
        else:
            # Return a regular response
            response = await agent_wrapper.acompletion(messages)

            # Ensure the content is a string
            content = response["choices"][0]["message"].get("content", "")
            if not isinstance(content, str):
                content = str(content)

            # Transform to proper OpenAI format
            return ChatCompletionResponse(
                model=request.model,
                choices=[
                    ChatCompletionChoice(
                        message={"role": "assistant", "content": content}
                    )
                ]
            )

    except Exception as e:
        logger.error(f"Error in chat completion: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/direct-sql")
async def direct_sql(request: DirectSQLRequest, background_tasks: BackgroundTasks):
    """Execute a direct SQL query via MCP (for testing)"""
    global agent_wrapper, mcp_session

    if agent_wrapper is None and mcp_session is None:
        raise HTTPException(status_code=500, detail="No MCP session available. Make sure MindsDB server is running with MCP enabled.")

    try:
        # First try to use the agent's session if available
        if hasattr(agent_wrapper.agent, "session") and agent_wrapper.agent.session:
            session = agent_wrapper.agent.session
            result = await session.call_tool("query", {"query": request.query})
            return {"result": result.content}
        # If agent session not available, use the direct session
        elif mcp_session:
            result = await mcp_session.call_tool("query", {"query": request.query})
            return {"result": result.content}
        else:
            raise HTTPException(status_code=500, detail="No MCP session available")

    except Exception as e:
        logger.error(f"Error executing direct SQL: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v1/models")
async def list_models():
    """List available models - always returns the single model we're using"""
    global agent_wrapper

    if agent_wrapper is None:
        return {
            "object": "list",
            "data": [
                {
                    "id": "mcp-agent",
                    "object": "model",
                    "created": 0,
                    "owned_by": "mindsdb"
                }
            ]
        }

    # Return the actual model name if available
    model_name = agent_wrapper.agent.args.get("model_name", "mcp-agent")

    return {
        "object": "list",
        "data": [
            {
                "id": model_name,
                "object": "model",
                "created": 0,
                "owned_by": "mindsdb"
            }
        ]
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    global agent_wrapper

    health_status = {
        "status": "ok",
        "agent_initialized": agent_wrapper is not None,
    }

    if agent_wrapper is not None:
        health_status["mcp_connected"] = hasattr(agent_wrapper.agent, "session") and agent_wrapper.agent.session is not None
        health_status["agent_name"] = agent_wrapper.agent.agent.name
        health_status["model_name"] = agent_wrapper.agent.args.get("model_name", "unknown")

    return health_status


@app.get("/test-mcp-connection")
async def test_mcp_connection():
    """Test the connection to the MCP server"""
    global mcp_session, exit_stack

    try:
        # If we already have a session, test it
        if mcp_session:
            try:
                tools_response = await mcp_session.list_tools()
                return {
                    "status": "ok",
                    "message": "Successfully connected to MCP server",
                    "tools": [tool.name for tool in tools_response.tools]
                }
            except Exception:
                # If error, close existing session and create a new one
                await exit_stack.aclose()
                mcp_session = None

        # Create a new MCP session - connect to running server
        server_params = StdioServerParameters(
            command="python",
            args=["-m", "mindsdb", "--api=mcp"],
            env=None
        )

        stdio_transport = await exit_stack.enter_async_context(stdio_client(server_params))
        stdio, write = stdio_transport
        session = await exit_stack.enter_async_context(ClientSession(stdio, write))

        await session.initialize()

        # Save the session for future use
        mcp_session = session

        # Get available tools
        tools_response = await session.list_tools()

        return {
            "status": "ok",
            "message": "Successfully connected to MCP server",
            "tools": [tool.name for tool in tools_response.tools]
        }
    except Exception as e:
        logger.error(f"Error connecting to MCP server: {str(e)}")
        error_detail = f"Error connecting to MCP server: {str(e)}. Make sure MindsDB server is running with MCP enabled: python -m mindsdb --api=mysql,mcp,http"
        raise HTTPException(status_code=500, detail=error_detail)


async def init_agent(agent_name: str, project_name: str, mcp_host: str, mcp_port: int):
    """Initialize the agent"""
    global agent_wrapper

    try:
        logger.info(f"Initializing MCP agent '{agent_name}' in project '{project_name}'")
        logger.info(f"Connecting to MCP server at {mcp_host}:{mcp_port}")
        logger.info("Make sure MindsDB server is running with MCP enabled: python -m mindsdb --api=mysql,mcp,http")

        agent_wrapper = create_mcp_agent(
            agent_name=agent_name,
            project_name=project_name,
            mcp_host=mcp_host,
            mcp_port=mcp_port
        )

        logger.info("Agent initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize agent: {str(e)}")
        return False


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on server shutdown"""
    global agent_wrapper, exit_stack

    if agent_wrapper:
        await agent_wrapper.cleanup()

    await exit_stack.aclose()


async def run_server_async(
    agent_name: str,
    project_name: str = "mindsdb",
    mcp_host: str = "127.0.0.1",
    mcp_port: int = 47337,
    host: str = "0.0.0.0",
    port: int = 8000
):
    """Run the FastAPI server"""
    # Initialize the agent
    success = await init_agent(agent_name, project_name, mcp_host, mcp_port)
    if not success:
        logger.error("Failed to initialize agent. Make sure MindsDB server is running with MCP enabled.")
        return 1

    return 0


def run_server(
    agent_name: str,
    project_name: str = "mindsdb",
    mcp_host: str = "127.0.0.1",
    mcp_port: int = 47337,
    host: str = "0.0.0.0",
    port: int = 8000
):
    """Run the FastAPI server"""
    logger.info("Make sure MindsDB server is running with MCP enabled: python -m mindsdb --api=mysql,mcp,http")
    # Initialize database
    from mindsdb.interfaces.storage import db
    db.init()

    # Run initialization in the event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(run_server_async(agent_name, project_name, mcp_host, mcp_port))
    if result != 0:
        return result
    # Run the server
    logger.info(f"Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a LiteLLM-compatible API server for MCP agent")
    parser.add_argument("--agent", type=str, required=True, help="Name of the agent to use")
    parser.add_argument("--project", type=str, default="mindsdb", help="Project containing the agent")
    parser.add_argument("--mcp-host", type=str, default="127.0.0.1", help="MCP server host")
    parser.add_argument("--mcp-port", type=int, default=47337, help="MCP server port")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind the server to")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the server on")

    args = parser.parse_args()

    run_server(
        agent_name=args.agent,
        project_name=args.project,
        mcp_host=args.mcp_host,
        mcp_port=args.mcp_port,
        host=args.host,
        port=args.port
    )
