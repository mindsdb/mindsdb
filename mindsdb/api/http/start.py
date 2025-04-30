import gc
gc.disable()

from flask import Flask
from waitress import serve

from mindsdb.api.http.initialize import initialize_app
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.utilities.functions import init_lexer_parsers
from mindsdb.integrations.libs.ml_exec_base import process_cache

gc.enable()

logger = log.getLogger(__name__)


def start(verbose, no_studio, app: Flask = None):
    db.init()
    init_lexer_parsers()

    if app is None:
        app = initialize_app(config, no_studio)

    port = config['api']['http']['port']
    host = config['api']['http']['host']
    server_type = config['api']['http']['server']['type']
    server_config = config['api']['http']['server']['config']

    process_cache.init()

    if server_type == "waitress":
        logger.debug("Serving HTTP app with waitress...")
        serve(
            app,
            host='*' if host in ('', '0.0.0.0') else host,
            port=port,
            **server_config
        )
    elif server_type == "flask":
        logger.debug("Serving HTTP app with flask...")
        # that will 'disable access' log in console

        app.run(debug=False, port=port, host=host, **server_config)
    elif server_type == 'gunicorn':
        try:
            from mindsdb.api.http.gunicorn_wrapper import StandaloneApplication
        except ImportError:
            logger.error(
                "Gunicorn server is not available by default. If you wish to use it, please install 'gunicorn'"
            )
            return

        def post_fork(arbiter, worker):
            db.engine.dispose()

        def before_worker_exit(arbiter, worker):
            """Latest version of gunicorn (23.0.0) calls 'join' for each child process before exiting. However this does
            not work for processes created by ProcessPoolExecutor, because they execute forever. We need to explicitly
            call 'shutdown' for such processes before exiting.
            """
            from mindsdb.integrations.libs.process_cache import process_cache
            process_cache.shutdown(wait=True)

        options = {
            'bind': f'{host}:{port}',
            'post_fork': post_fork,
            'worker_exit': before_worker_exit,
            **server_config
        }
        StandaloneApplication(app, options).run()
