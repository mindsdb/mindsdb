import os

from mindsdb.integrations.handlers_wrapper.socketio_server import create_server_app, web
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.log import get_log


def get_db_handler_instance(class_name, **kwargs):
    handler_class = get_handler(class_name)
    handler = handler_class(**kwargs)
    return handler


if __name__ == '__main__':
    app = create_server_app(get_db_handler_instance)

    logger = get_log(logger_name="main")
    port = os.environ.get("PORT", 5001)
    host = os.environ.get("HOST", "0.0.0.0")
    logger.info("Running service: host=%s, port=%s", host, port)
    web.run_app(app, host=host, port=port)
