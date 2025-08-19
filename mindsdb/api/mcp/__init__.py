import os
from typing import Any
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from mindsdb.interfaces.storage import db

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
    dependencies=["mindsdb"],  # Add any additional dependencies

)

class CustomAuthMiddleware(BaseHTTPMiddleware):
    """Custom middleware to handle authentication basing on header 'Authorization'"""

    async def dispatch(self, request: Request, call_next):
        mcp_access_token = os.environ.get("MINDSDB_MCP_ACCESS_TOKEN")
        if mcp_access_token is not None:
            auth_token = request.headers.get("Authorization", "").partition("Bearer ")[-1]
            if mcp_access_token != auth_token:
                return Response(status_code=401, content="Unauthorized", media_type="text/plain")

        response = await call_next(request)

        return response

def get_mcp_app(host: str = None, port: int = None):
    if host is not None:
        mcp.settings.host = host
    if port is not None:
        mcp.settings.port = port
    app = mcp.sse_app()
    app.add_middleware(CustomAuthMiddleware)
    return app