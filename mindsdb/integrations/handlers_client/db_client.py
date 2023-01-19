import os
from mindsdb.utilities.log import get_log

logger = get_log()


class DBServiceClient:
    def __init__(self, handler_type, **handler_ars):
        from mindsdb.integrations.handlers_wrapper.socketio_server import SocketIOClient

        self.instance = SocketIOClient(os.environ.get("MINDSDB_DB_SERVICE_URL"))

        # remove unserializable params
        handler_ars.pop('file_controller', None)
        handler_ars.pop('file_storage', None)

        self.instance.init(handler_type, **handler_ars)

    def __getattr__(self, method_name):
        if not method_name.startswith('_'):
            return getattr(self.instance, method_name)

    def __del__(self):
        self.instance.close()

