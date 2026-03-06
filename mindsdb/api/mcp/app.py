from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from mindsdb.utilities.config import config
from mindsdb.api.mcp.mcp_instance import mcp

# region these imports required for correct initialization
from mindsdb.api.mcp import tools  # noqa: F401
from mindsdb.api.mcp import resources  # noqa: F401
from mindsdb.api.mcp import prompts  # noqa: F401
from mindsdb.api.mcp import completions  # noqa: F401
# endregion


def _get_status(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "mindsdb-mcp"})


def get_mcp_app():
    sse_starlette = mcp.sse_app()
    http_starlette = mcp.streamable_http_app()

    @asynccontextmanager
    async def lifespan(_):
        """Required for streamable_http to run task group"""
        async with http_starlette.router.lifespan_context(http_starlette):
            yield

    combined_app = Starlette(
        routes=list(sse_starlette.routes) + list(http_starlette.routes),
        lifespan=lifespan,
    )

    if config["api"]["mcp"]["cors"]["enabled"]:
        combined_app.add_middleware(
            CORSMiddleware,
            allow_origins=config["api"]["mcp"]["cors"]["allow_origins"],
            allow_origin_regex=config["api"]["mcp"]["cors"]["allow_origin_regex"],
            allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
            allow_headers=config["api"]["mcp"]["cors"]["allow_headers"],
            expose_headers=["mcp-session-id"],
        )

    combined_app.add_route("/status", _get_status, methods=["GET"])

    return combined_app
