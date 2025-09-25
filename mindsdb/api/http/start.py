import gc

gc.disable()

from flask import Flask

from mindsdb.api.http.initialize import initialize_app
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.utilities.functions import init_lexer_parsers
from mindsdb.integrations.libs.ml_exec_base import process_cache
from mindsdb.api.common.middleware import PATAuthMiddleware


from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.middleware.wsgi import WSGIMiddleware
import uvicorn

from mindsdb.api.a2a import get_a2a_app
from mindsdb.api.mcp import get_mcp_app

gc.enable()

logger = log.getLogger(__name__)


def start(verbose, app: Flask = None):
    db.init()
    init_lexer_parsers()

    if app is None:
        app = initialize_app()

    port = config["api"]["http"]["port"]
    host = config["api"]["http"]["host"]

    process_cache.init()

    routes = []
    # Specific mounts FIRST
    a2a = get_a2a_app()
    a2a.add_middleware(PATAuthMiddleware)
    mcp = get_mcp_app()
    mcp.add_middleware(PATAuthMiddleware)
    routes.append(Mount("/a2a", app=a2a))
    routes.append(Mount("/mcp", app=mcp))

    # Root app LAST so it won't shadow the others
    routes.append(Mount("/", app=WSGIMiddleware(app)))

    # Setting logging to None makes uvicorn use the existing logging configuration
    uvicorn.run(Starlette(routes=routes, debug=verbose), host=host, port=int(port), log_level=None, log_config=None)
