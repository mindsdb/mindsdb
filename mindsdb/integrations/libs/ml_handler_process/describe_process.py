import importlib
from typing import Optional, Union

from pandas import DataFrame

from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage


def describe_process(integration_id: int, attribute: Optional[Union[str, list]],
                     model_id: int, module_path: str) -> DataFrame:
    module = importlib.import_module(module_path)

    handlerStorage = HandlerStorage(integration_id)
    modelStorage = ModelStorage(model_id)

    ml_handler = module.Handler(
        engine_storage=handlerStorage,
        model_storage=modelStorage
    )
    df = ml_handler.describe(attribute=attribute)
    return df
