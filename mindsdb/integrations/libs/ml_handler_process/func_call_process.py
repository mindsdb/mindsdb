import importlib

from mindsdb.interfaces.storage.model_fs import HandlerStorage


def func_call_process(name: str, args: dict, integration_id: int, module_path: str) -> None:
    module = importlib.import_module(module_path)

    if module.import_error is not None:
        raise module.import_error

    result = None

    if hasattr(module.Handler, 'function_call'):
        engine_storage = HandlerStorage(integration_id)
        try:
            result = module.Handler(
                engine_storage=engine_storage,
                model_storage=None
            ).function_call(name, args)
        except NotImplementedError:
            return None
        except Exception as e:
            raise e

    return result
