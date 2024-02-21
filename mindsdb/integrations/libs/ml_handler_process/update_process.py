import importlib

from mindsdb.interfaces.storage.model_fs import HandlerStorage, ModelStorage


def update_process(args: dict, integration_id: int, module_path: str, model_id: int) -> None:
    module = importlib.import_module(module_path)

    if module.import_error is not None:
        raise module.import_error

    result = None

    if hasattr(module.Handler, 'upgate'):
        engine_storage = HandlerStorage(integration_id)
        model_storage = ModelStorage(model_id)
        try:
            result = module.Handler(
                engine_storage=engine_storage,
                model_storage=model_storage
            ).upgate(args=args)
        except NotImplementedError:
            return None
        except Exception as e:
            raise e

    return result
