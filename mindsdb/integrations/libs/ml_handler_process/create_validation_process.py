import importlib

from mindsdb.interfaces.storage.model_fs import HandlerStorage


def create_validation_process(payload, dataframe):
    target = payload.get('target')
    args = payload.get('args')
    integration_id = payload.get('integration_id')

    module = importlib.import_module(payload['handler_meta']['module_path'])

    if hasattr(module.Handler, 'create_validation'):
        module.Handler.create_validation(
            target,
            args=args,
            handler_storage=HandlerStorage(integration_id)
        )
