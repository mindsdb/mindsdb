import importlib

from mindsdb.interfaces.storage.model_fs import HandlerStorage


def create_validation_process(target: str, args: dict, integration_id: int, module_path: str) -> None:
    module = importlib.import_module(module_path)

    if hasattr(module.Handler, 'create_validation'):
        module.Handler.create_validation(
            target,
            args=args,
            handler_storage=HandlerStorage(integration_id)
        )
