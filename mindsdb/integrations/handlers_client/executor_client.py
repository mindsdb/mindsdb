import os


class ExecuteCommandsClient:
    def __init__(self, session=None):
        from mindsdb.integrations.handlers_wrapper.socketio_server import SocketIOClient

        self.instance = SocketIOClient(os.environ.get("MINDSDB_EXECUTOR_URL"))

        self.instance.init()

    def execute_command(self, statement, server_context=None):
        return self.instance.execute_command(statement, server_context)

    def __del__(self):
        self.instance.close()
