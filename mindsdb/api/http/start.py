import logging
import multiprocessing as mp
import os

from waitress import serve

from mindsdb.api.http.initialize import initialize_app
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import init_lexer_parsers
from mindsdb.integrations.libs.ml_exec_base import process_cache
from mindsdb.interfaces.database.integrations import integration_controller


def start(verbose, no_studio, with_nlp):
    log.configure_logging()  # Because this is the entrypoint for a process, we need to config logging
    logger = logging.getLogger(__name__)
    logger.info("HTTP API is starting..")
    config = Config()
    is_cloud = config.get('cloud', False)

    server = os.environ.get("MINDSDB_DEFAULT_SERVER", "waitress")
    db.init()
    init_lexer_parsers()

    app = initialize_app(config, no_studio, with_nlp)

    port = config["api"]["http"]["port"]
    host = config["api"]["http"]["host"]

    # region preload ml handlers
    preload_hendlers = {}

    lightwood_handler = integration_controller.handler_modules['lightwood']
    if lightwood_handler.Handler is not None:
        preload_hendlers[lightwood_handler.Handler] = 4 if is_cloud else 1

    huggingface_handler = integration_controller.handler_modules['huggingface']
    if huggingface_handler.Handler is not None:
        preload_hendlers[huggingface_handler.Handler] = 1 if is_cloud else 0

    openai_handler = integration_controller.handler_modules['openai']
    if openai_handler.Handler is not None:
        preload_hendlers[openai_handler.Handler] = 1 if is_cloud else 0

    process_cache.init(preload_hendlers)
    # endregion

    if server.lower() == 'waitress':
        logger.debug("Serving HTTP app with waitres..")
        serve(
            app,
            host="*" if host in ("", "0.0.0.0") else host,
            port=port,
            threads=16,
            max_request_body_size=1073741824 * 10,
            inbuf_overflow=1073741824 * 10,
        )
    elif server.lower() == "flask":
        logger.debug("Serving HTTP app with flask..")
        # that will 'disable access' log in console
        # logger = logging.getLogger("werkzeug")
        # logger.setLevel(logging.WARNING)

        app.run(
            debug=False,
            port=port,
            host=host,
            use_reloader=True,
            use_debugger=True,
            passthrough_errors=True,
        )
    elif server.lower() == "gunicorn":
        logger.debug("Serving HTTP app with gunicorn..")
        try:
            from mindsdb.api.http.gunicorn_wrapper import StandaloneApplication
        except ImportError:
            logger.error(
                "Gunicorn server is not available by default. If you wish to use it, please install 'gunicorn'"
            )
            return

        def post_fork(arbiter, worker):
            db.engine.dispose()

        options = {
            "bind": f"{host}:{port}",
            "workers": mp.cpu_count(),
            "timeout": 600,
            "reuse_port": True,
            "preload_app": True,
            "post_fork": post_fork,
            "threads": 4,
        }
        StandaloneApplication(app, options).run()
