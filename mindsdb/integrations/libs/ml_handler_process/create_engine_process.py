import importlib

from pandas import DataFrame

from mindsdb.interfaces.storage.model_fs import HandlerStorage


def create_engine_process(connection_args: dict, integration_id: int, module_path: str) -> DataFrame:
    module = importlib.import_module(module_path)

    if module.import_error is not None:
        raise module.import_error

    result = None

    if hasattr(module.Handler, "create_engine"):
        engine_storage = HandlerStorage(integration_id)
        try:
            result = module.Handler(engine_storage=engine_storage, model_storage=None).create_engine(
                connection_args=connection_args
            )
        except NotImplementedError:
            return None

    return result
