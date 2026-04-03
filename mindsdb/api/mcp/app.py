from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend
from mcp.server.auth.middleware.auth_context import AuthContextMiddleware

from mindsdb.utilities.config import config
from mindsdb.api.common.middleware import RateLimitMiddleware
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

    middleware = []

    # Preserve AuthenticationMiddleware from http_starlette so that
    # RequireAuthMiddleware can read scope["user"] set by BearerAuthBackend.
    if mcp._token_verifier is not None:
        middleware = [
            Middleware(AuthenticationMiddleware, backend=BearerAuthBackend(mcp._token_verifier)),
            Middleware(AuthContextMiddleware),
        ]

    combined_app = Starlette(
        routes=list(sse_starlette.routes) + list(http_starlette.routes),
        middleware=middleware,
        lifespan=lifespan,
    )

    # Rate limit should be added before CORS, so that CORS adds correct headers
    if config["api"]["mcp"]["rate_limit"]["enabled"]:
        combined_app.add_middleware(
            RateLimitMiddleware,
            requests_per_minute=config["api"]["mcp"]["rate_limit"]["requests_per_minute"],
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


def get_mcp_well_known_routes() -> list[Route]:
    """Return OAuth protected resource metadata routes for mounting at the server root.

    RFC 9728 requires /.well-known/oauth-protected-resource to be served at the
    server root, not under the /mcp sub-path, so start.py registers these separately.
    """
    from mcp.server.auth.routes import create_protected_resource_routes

    auth = mcp.settings.auth
    if not auth or not auth.resource_server_url:
        return []

    return create_protected_resource_routes(
        resource_url=auth.resource_server_url,
        authorization_servers=[auth.issuer_url],
        scopes_supported=auth.required_scopes,
    )
