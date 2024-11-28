import os
from typing import Dict

from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.utilities.config import Config

"""Contains utilities to be used by handlers."""


def get_api_key(
    api_name: str,
    create_args: Dict[str, str],
    engine_storage: HandlerStorage = None,
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
        1. provided at inference
        2. provided at model creation
        3. provided at engine creation
        4. api key env variable
        5. api_key setting in config.json
    """
    # Special case for vLLM - always return dummy key
    if api_name == "vllm":
        return "EMPTY"

    # 1
    if "using" in create_args and f"{api_name.lower()}_api_key" in create_args["using"]:
        return create_args["using"][f"{api_name.lower()}_api_key"]

    # 2
    if f"{api_name.lower()}_api_key" in create_args:
        return create_args[f"{api_name.lower()}_api_key"]

    # 3
    if engine_storage is not None:
        connection_args = engine_storage.get_connection_args()
        if f"{api_name.lower()}_api_key" in connection_args:
            return connection_args[f"{api_name.lower()}_api_key"]

    # 4
    api_key = os.getenv(f"{api_name.lower()}_api_key")
    if api_key is not None:
        return api_key
    api_key = os.getenv(f"{api_name.upper()}_API_KEY")
    if api_key is not None:
        return api_key

    # 5
    config = Config()
    api_cfg = config.get(api_name, {})
    if f"{api_name.lower()}_api_key" in api_cfg:
        return api_cfg[f"{api_name.lower()}_api_key"]

    if strict:
        raise Exception(
            f"Missing API key '{api_name.lower()}_api_key'. Either re-create this ML_ENGINE specifying the '{api_name.lower()}_api_key' parameter, or re-create this model and pass the API key with `USING` syntax."
        )  # noqa
    return None
