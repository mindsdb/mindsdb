import importlib

from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage


def describe_process(payload, model_id):
    handler_meta = payload.get('handler_meta')
    # handler_meta = {'module_path': 'mindsdb.integrations...od_handler', 'class_name': 'Handler', 'engine': 'lightwood', 'integration_id': 1}
    attribute = payload.get('attribute')

    module = importlib.import_module(handler_meta['module_path'])

    handlerStorage = HandlerStorage(handler_meta['integration_id'])
    modelStorage = ModelStorage(model_id)

    ml_handler = module.Handler(
        engine_storage=handlerStorage,
        model_storage=modelStorage
    )
    df = ml_handler.describe(attribute=attribute)
    return df
