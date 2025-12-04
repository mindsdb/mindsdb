import gc
import os

gc.disable()

from flask import Flask
import uvicorn
from a2wsgi import WSGIMiddleware

from mindsdb.api.http.initialize import initialize_app
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.utilities.functions import init_lexer_parsers
from mindsdb.integrations.libs.ml_exec_base import process_cache
from mindsdb.api.common.middleware import PATAuthMiddleware

gc.enable()

logger = log.getLogger(__name__)


def _is_http_only_mode() -> bool:
    """Return True when the user explicitly asked for only the HTTP API."""

    apis = config.cmd_args.api

    if apis is None:
        return False

    enabled = {name.strip().lower() for name in apis.split(",") if name.strip()}
    return enabled == {"http"}


def _wrap_flask_app(app: Flask):
    """Convert the Flask WSGI app into an ASGI-compatible callable."""

    a2wsgi_cfg = config["api"]["http"]["a2wsgi"]
    return WSGIMiddleware(
        app,
        workers=a2wsgi_cfg["workers"],
        send_queue_size=a2wsgi_cfg["send_queue_size"],
    )


def start(verbose, app: Flask = None, is_restart: bool = False):
    db.init()
    init_lexer_parsers()

    if app is None:
        app = initialize_app(is_restart)

    port = config["api"]["http"]["port"]
    host = config["api"]["http"]["host"]

    process_cache.init()

    http_only_mode = _is_http_only_mode()

    if not http_only_mode:
        from starlette.applications import Starlette
        from starlette.routing import Mount
        from mindsdb.api.a2a import get_a2a_app
        from mindsdb.api.mcp import get_mcp_app

        a2a = get_a2a_app()
        a2a.add_middleware(PATAuthMiddleware)
        mcp = get_mcp_app()
        mcp.add_middleware(PATAuthMiddleware)
        routes = [
            Mount("/a2a", app=a2a),
            Mount("/mcp", app=mcp),
            Mount("/", app=_wrap_flask_app(app)),
        ]

        root_app = Starlette(routes=routes, debug=verbose)
    else:
        root_app = _wrap_flask_app(app)

    # Setting logging to None makes uvicorn use the existing logging configuration
    uvicorn.run(
        root_app,
        host=host,
        port=int(port),
        log_level=None,
        log_config=None,
    )
