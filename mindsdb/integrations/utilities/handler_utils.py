import os
from typing import Dict

from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.utilities.config import Config

"""Contains utilities to be used by handlers."""


def get_api_key(
    api_name: str,
    create_args: Dict[str, str],
    engine_storage: HandlerStorage,
    strict: bool = True,
):
    """Gets the API key needed to use an ML Handler.

    Args:
        api_name (str): Name of the API (e.g. openai, anthropic)
        create_args (Dict[str, str]): Args user passed to the created model with USING keyword
        engine_storage (HandlerStorage): Engine storage for the ML handler
        strict (bool): Whether or not to require the API key

    Returns:
        api_key (str): The API key

    API_KEY preference order:
        1. provided at model creation
        2. provided at engine creation
        3. api key env variable
        4. api_key setting in config.json
    """
    # 1
    if "api_key" in create_args:
        return create_args["api_key"]
    # 2
    connection_args = engine_storage.get_connection_args()
    if "api_key" in connection_args:
        return connection_args["api_key"]
    # 3
    api_key = os.getenv(f"{api_name.upper()}_API_KEY")
    if api_key is not None:
        return api_key
    # 4
    config = Config()
    api_cfg = config.get(api_name, {})
    if "api_key" in api_cfg:
        return api_cfg["api_key"]

    if strict:
        raise Exception(
            'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
            or re-create this model and pass the API key with `USING` syntax.'
        )  # noqa
    return None
