import os

from mindsdb.integrations.handlers_wrapper.socketio_server import create_server_app, web
from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands


if __name__ == '__main__':
    app = create_server_app(ExecuteCommands)

    web.run_app(app, port=os.environ.get("PORT", 5500))
