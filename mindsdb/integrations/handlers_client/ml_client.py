import os
from mindsdb.utilities.log import get_log

logger = get_log()


class MLServiceClient:
    def __init__(self, class_path, integration_id, predictor_id):
        from mindsdb.integrations.handlers_wrapper.socketio_server import SocketIOClient

        self.instance = SocketIOClient(os.environ.get("MINDSDB_ML_SERVICE_URL"))

        self.instance.init(class_path, integration_id, predictor_id)

    def __getattr__(self, method_name):
        if not method_name.startswith('_'):
            return getattr(self.instance, method_name)

    def __del__(self):
        self.instance.close()
