import gc

gc.disable()

from flask import Flask
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Route
from starlette.responses import RedirectResponse
from starlette.routing import Mount
from a2wsgi import WSGIMiddleware
import uvicorn

from mindsdb.api.http.initialize import initialize_app
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.utilities.functions import init_lexer_parsers
from mindsdb.integrations.libs.ml_exec_base import process_cache
from mindsdb.api.common.middleware import PATAuthMiddleware
from mindsdb.api.a2a import get_a2a_app
from mindsdb.api.mcp import get_mcp_app

gc.enable()

logger = log.getLogger(__name__)


def start(verbose, app: Flask = None, is_restart: bool = False):
    db.init()
    init_lexer_parsers()

    if app is None:
        app = initialize_app(is_restart)

    port = config["api"]["http"]["port"]
    host = config["api"]["http"]["host"]

    process_cache.init()

    routes = []
    # Specific mounts FIRST
    a2a = get_a2a_app()
    a2a.add_middleware(PATAuthMiddleware)
    mcp = get_mcp_app()
    mcp.add_middleware(PATAuthMiddleware)
    # Redirect '/a2a' to '/a2a/' with 307 to preserve method & body
    routes.append(
        Route(
            "/a2a", lambda request: RedirectResponse(url="/a2a/", status_code=307), methods=["GET", "POST", "OPTIONS"]
        )
    )
    # Mount the A2A app at a trailing-slash path to avoid 405 on POST without slash
    routes.append(Mount("/a2a/", app=a2a))
    routes.append(Mount("/mcp", app=mcp))

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

    # Build top-level ASGI app and apply CORS so errors (e.g., 401) also include CORS headers
    top_app = Starlette(routes=routes, debug=verbose)
    cors_cfg = config.get("api", {}).get("cors", {})
    allowed_origins = cors_cfg.get(
        "allow_origins",
        [
            "http://localhost:3001",
            "http://127.0.0.1:3001",
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ],
    )
    top_app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Setting logging to None makes uvicorn use the existing logging configuration
    uvicorn.run(top_app, host=host, port=int(port), log_level=None, log_config=None)
