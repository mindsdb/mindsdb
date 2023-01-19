import os
import importlib
from mindsdb.integrations.handlers_wrapper.socketio_server import create_server_app, web
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage


def get_db_handler_instance(class_path, integration_id, predictor_id):

    module_name, class_name = class_path
    module = importlib.import_module(module_name)
    HandlerClass = getattr(module, class_name)

    handlerStorage = HandlerStorage(integration_id)
    modelStorage = ModelStorage(predictor_id)

    ml_handler = HandlerClass(
        engine_storage=handlerStorage,
        model_storage=modelStorage,
    )

    return ml_handler


if __name__ == '__main__':
    app = create_server_app(get_db_handler_instance)

    web.run_app(app, port=os.environ.get("PORT", 5001))
