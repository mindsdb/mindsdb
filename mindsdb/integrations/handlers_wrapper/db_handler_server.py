import os

from mindsdb.integrations.handlers_wrapper.socketio_server import create_server_app, web
from mindsdb.integrations.libs.handler_helpers import get_handler


def get_db_handler_instance(class_name, **kwargs):
    handler_class = get_handler(class_name)
    handler = handler_class(**kwargs)
    return handler


if __name__ == '__main__':
    app = create_server_app(get_db_handler_instance)

    web.run_app(app, port=os.environ.get("PORT", 5001))
