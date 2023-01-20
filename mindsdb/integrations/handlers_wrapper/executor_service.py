import os

from mindsdb.integrations.handlers_wrapper.socketio_server import create_server_app, web
from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands
from mindsdb.utilities.log import get_log


def get_executor_instance(*args, **kwargs):
    return ExecuteCommands(*args, **kwargs)


if __name__ == '__main__':
    app = create_server_app(ExecuteCommands)
    logger = get_log(logger_name="main")
    port = os.environ.get("PORT", 5500)
    host = os.environ.get("HOST", "0.0.0.0")
    logger.info("Running service: host=%s, port=%s", host, port)
    web.run_app(app, host=host, port=port)
