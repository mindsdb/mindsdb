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
        api_key = create_args["using"][f"{api_name.lower()}_api_key"]
        if api_key:
            return api_key

    # 1.5 - Check for generic api_key in using
    if "using" in create_args and "api_key" in create_args["using"]:
        api_key = create_args["using"]["api_key"]
        if api_key:
            return api_key

    # 2
    if f"{api_name.lower()}_api_key" in create_args:
        api_key = create_args[f"{api_name.lower()}_api_key"]
        if api_key:
            return api_key

    # 2.5 - Check for generic api_key
    if "api_key" in create_args:
        api_key = create_args["api_key"]
        if api_key:
            return api_key

    # 3 - Check in params dictionary if it exists (for agents)
    if "params" in create_args and create_args["params"] is not None:
        if f"{api_name.lower()}_api_key" in create_args["params"]:
            api_key = create_args["params"][f"{api_name.lower()}_api_key"]
            if api_key:
                return api_key
        # 3.5 - Check for generic api_key in params
        if "api_key" in create_args["params"]:
            api_key = create_args["params"]["api_key"]
            if api_key:
                return api_key

    # 4
    if engine_storage is not None:
        connection_args = engine_storage.get_connection_args()
        if f"{api_name.lower()}_api_key" in connection_args:
            api_key = connection_args[f"{api_name.lower()}_api_key"]
            if api_key:
                return api_key
        # 4.5 - Check for generic api_key in connection_args
        if "api_key" in connection_args:
            api_key = connection_args["api_key"]
            if api_key:
                return api_key

    # 5
    api_key = os.getenv(f"{api_name.lower()}_api_key")
    if api_key:
        return api_key
    api_key = os.getenv(f"{api_name.upper()}_API_KEY")
    if api_key:
        return api_key

    # 6
    config = Config()
    api_cfg = config.get(api_name, {})
    if f"{api_name.lower()}_api_key" in api_cfg:
        api_key = api_cfg[f"{api_name.lower()}_api_key"]
        if api_key:
            return api_key

    # 7
    if "api_keys" in create_args and api_name in create_args["api_keys"]:
        api_key = create_args["api_keys"][api_name]
        if api_key:
            return api_key

    if strict:
        provider_upper = api_name.upper()
        api_key_env_var = f"{provider_upper}_API_KEY"
        api_key_arg = f"{api_name.lower()}_api_key"
        error_message = (
            f"API key for {api_name} not found. Please provide it using one of the following methods:\n"
            f"1. Set the {api_key_env_var} environment variable\n"
            f"2. Provide it as '{api_key_arg}' parameter or 'api_key' parameter when creating an agent using the CREATE AGENT syntax\n"
            f"   Example: CREATE AGENT my_agent USING model='gpt-4', provider='{api_name}', {api_key_arg}='your-api-key';\n"
            f"   Or: CREATE AGENT my_agent USING model='gpt-4', provider='{api_name}', api_key='your-api-key';\n"
        )
        raise Exception(error_message)
    return None
