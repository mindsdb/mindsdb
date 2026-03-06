from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from mindsdb.api.mcp.mcp_instance import mcp
from mindsdb.api.mcp import tools  # noqa: F401 — registers all tools via @mcp.tool


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

    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
            allow_headers=["*"],
            expose_headers=["mcp-session-id"],
        ),
    ]

    # Preserve AuthenticationMiddleware from http_starlette so that
    # RequireAuthMiddleware can read scope["user"] set by BearerAuthBackend.
    if mcp._token_verifier is not None:
        from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend
        from mcp.server.auth.middleware.auth_context import AuthContextMiddleware
        middleware = [
            Middleware(AuthenticationMiddleware, backend=BearerAuthBackend(mcp._token_verifier)),
            Middleware(AuthContextMiddleware),
        ] + middleware

    combined_app = Starlette(
        routes=list(sse_starlette.routes) + list(http_starlette.routes),
        middleware=middleware,
        lifespan=lifespan,
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
