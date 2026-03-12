import gc
from importlib import import_module

gc.disable()

from flask import Flask
from starlette.applications import Starlette
from starlette.routing import Mount, Route
from starlette.responses import JSONResponse
from a2wsgi import WSGIMiddleware
import uvicorn

from mindsdb.api.http.initialize import initialize_app
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.utilities.functions import init_lexer_parsers
from mindsdb.integrations.libs.ml_exec_base import process_cache
from mindsdb.api.common.middleware import PATAuthMiddleware

gc.enable()

logger = log.getLogger(__name__)


async def _health_check(request):
    """Async health check that bypasses the WSGI worker pool for the mindsdb API."""
    return JSONResponse({"status": "ok"})


def _mount_optional_api(name: str, mount_path: str, get_app_fn, routes):
    try:
        optional_app = get_app_fn()
    except ImportError as exc:
        logger.warning(
            "%s support is disabled (%s). To enable it, install the %s extra: pip install 'mindsdb[%s]'",
            name,
            exc,
            name,
            name.lower(),
        )
        return

    optional_app.add_middleware(PATAuthMiddleware)
    routes.append(Mount(mount_path, app=optional_app))


def start(verbose, app: Flask = None, is_restart: bool = False):
    db.init()
    init_lexer_parsers()

    if app is None:
        app = initialize_app(is_restart)

    port = config["api"]["http"]["port"]
    host = config["api"]["http"]["host"]

    process_cache.init()

    routes = []

    # Health check FIRST - async endpoint that bypasses WSGI worker pool
    # This ensures health checks respond even when all workers are blocked
    routes.append(Route("/api/util/ping", _health_check, methods=["GET"]))

    _mount_optional_api(
        "A2A",
        "/a2a",
        lambda: import_module("mindsdb.api.a2a").get_a2a_app(),
        routes,
    )
    _mount_optional_api(
        "MCP",
        "/mcp",
        lambda: import_module("mindsdb.api.mcp").get_mcp_app(),
        routes,
    )

    # Root app LAST so it won't shadow the others
    routes.append(
        Mount(
            "/",
            app=WSGIMiddleware(
                app,
                workers=config["api"]["http"]["a2wsgi"]["workers"],
                send_queue_size=config["api"]["http"]["a2wsgi"]["send_queue_size"],
            ),
        )
    )

    # Setting logging to None makes uvicorn use the existing logging configuration
    uvicorn.run(Starlette(routes=routes, debug=verbose), host=host, port=int(port), log_level=None, log_config=None)
