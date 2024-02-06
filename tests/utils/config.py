import os
import random
import json


def generate_random_db_name(base_name: str, min_value: int = 1000, max_value: int = 9999) -> str:
    """
    Generates a random database name by appending a random number to the base name.

    Args:
        base_name (str): The base name for the database.
        min_value (int, optional): The minimum value for the random number (inclusive). Defaults to 1000.
        max_value (int, optional): The maximum value for the random number (inclusive). Defaults to 9999.

    Returns:
        str: The generated database name.
    """
    random_number = random.randint(min_value, max_value)
    return f"{base_name}_{random_number}"


def get_value_from_json_env_var(env_var_name: str, key: str):
    """
    Retrieve a value from a JSON string stored in as an environment variable.

    Args:
        env_var_name (str): The name of the environment variable.
        key (str): The key in the JSON object to retrieve.

    Raises:
        EnvironmentError: If the environment variable is not set or is not a valid JSON string.
        KeyError: If the key doesn't exist in the parsed dictionary.

    Returns:
        The value associated with the provided key.
    """

    json_str = os.environ.get(env_var_name)

    if json_str is None:
        raise EnvironmentError(f'Environment variable {env_var_name} is not set.')

    try:
        parsed_dict = json.loads(json_str)
    except json.JSONDecodeError:
        raise EnvironmentError(f'Environment variable {env_var_name} is not a valid JSON string.')

    if key not in parsed_dict:
        raise KeyError(f"Key '{key}' not found in environment variable {env_var_name}")
    return parsed_dict[key]
