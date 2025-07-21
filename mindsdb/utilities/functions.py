import os
import base64
import hashlib
import json
import datetime
import textwrap
from functools import wraps
from collections.abc import Callable

from cryptography.fernet import Fernet
from mindsdb_sql_parser.ast import Identifier

from mindsdb.utilities.fs import create_process_mark, delete_process_mark, set_process_mark
from mindsdb.utilities import log
from mindsdb.utilities.config import Config


logger = log.getLogger(__name__)


def get_handler_install_message(handler_name):
    if Config().use_docker_env:
        container_id = os.environ.get("HOSTNAME", "<container_id>")
        return textwrap.dedent(f"""\
            To install the {handler_name} handler, run the following in your terminal outside the docker container
            ({container_id} is the ID of this container):

                docker exec {container_id} pip install 'mindsdb[{handler_name}]'""")
    else:
        return textwrap.dedent(f"""\
            To install the {handler_name} handler, run the following in your terminal:

                pip install 'mindsdb[{handler_name}]'  # If you installed mindsdb via pip
                pip install '.[{handler_name}]'        # If you installed mindsdb from source""")


def cast_row_types(row, field_types):
    """ """
    keys = [x for x in row.keys() if x in field_types]
    for key in keys:
        t = field_types[key]
        if t == "Timestamp" and isinstance(row[key], (int, float)):
            timestamp = datetime.datetime.fromtimestamp(row[key], datetime.timezone.utc)
            row[key] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        elif t == "Date" and isinstance(row[key], (int, float)):
            timestamp = datetime.datetime.fromtimestamp(row[key], datetime.timezone.utc)
            row[key] = timestamp.strftime("%Y-%m-%d")
        elif t == "Int" and isinstance(row[key], (int, float, str)):
            try:
                logger.debug(f"cast {row[key]} to {int(row[key])}")
                row[key] = int(row[key])
            except Exception:
                pass


def mark_process(name: str, custom_mark: str = None) -> Callable:
    def mark_process_wrapper(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            if custom_mark is None:
                mark = create_process_mark(name)
            else:
                mark = set_process_mark(name, custom_mark)

            try:
                return func(*args, **kwargs)
            finally:
                delete_process_mark(name, mark)

        return wrapper

    return mark_process_wrapper


def init_lexer_parsers():
    from mindsdb_sql_parser.lexer import MindsDBLexer
    from mindsdb_sql_parser.parser import MindsDBParser

    return MindsDBLexer(), MindsDBParser()


def resolve_table_identifier(identifier: Identifier, default_database: str = None) -> tuple:
    parts = identifier.parts

    parts_count = len(parts)
    if parts_count == 1:
        return (None, parts[0])
    elif parts_count == 2:
        return (parts[0], parts[1])
    else:
        raise Exception(f"Table identifier must contain max 2 parts: {parts}")


def resolve_model_identifier(identifier: Identifier) -> tuple:
    """
    Splits a model identifier into its database, model name, and version components.

    The identifier may contain one, two, or three parts.
    The function supports both quoted and unquoted identifiers, and normalizes names to lowercase if unquoted.

    Examples:
        >>> resolve_model_identifier(Identifier(parts=['a', 'b']))
        ('a', 'b', None)
        >>> resolve_model_identifier(Identifier(parts=['a', '1']))
        (None, 'a', 1)
        >>> resolve_model_identifier(Identifier(parts=['a']))
        (None, 'a', None)
        >>> resolve_model_identifier(Identifier(parts=['a', 'b', 'c']))
        (None, None, None)  # not found

    Args:
        identifier (Identifier): The identifier object containing parts and is_quoted attributes.

    Returns:
        tuple: (database_name, model_name, model_version)
            - database_name (str or None): The name of the database/project, or None if not specified.
            - model_name (str or None): The name of the model, or None if not found.
            - model_version (int or None): The model version as an integer, or None if not specified.
    """
    model_name = None
    db_name = None
    version = None
    model_name_quoted = None
    db_name_quoted = None

    match identifier.parts, identifier.is_quoted:
        case [model_name], [model_name_quoted]:
            ...
        case [model_name, str(version)], [model_name_quoted, _] if version.isdigit():
            ...
        case [model_name, int(version)], [model_name_quoted, _]:
            ...
        case [db_name, model_name], [db_name_quoted, model_name_quoted]:
            ...
        case [db_name, model_name, str(version)], [db_name_quoted, model_name_quoted, _] if version.isdigit():
            ...
        case [db_name, model_name, int(version)], [db_name_quoted, model_name_quoted, _]:
            ...
        case [db_name, model_name, str(version)], [db_name_quoted, model_name_quoted, _]:
            # for back compatibility. May be delete?
            return (None, None, None)
        case _:
            ...  # may be raise ValueError?

    if model_name_quoted is False:
        model_name = model_name.lower()

    if db_name_quoted is False:
        db_name = db_name.lower()

    if isinstance(version, int) or isinstance(version, str) and version.isdigit():
        version = int(version)
    else:
        version = None

    return db_name, model_name, version


def encrypt(string: bytes, key: str) -> bytes:
    hashed_string = hashlib.sha256(key.encode()).digest()

    fernet_key = base64.urlsafe_b64encode(hashed_string)

    cipher = Fernet(fernet_key)
    return cipher.encrypt(string)


def decrypt(encripted: bytes, key: str) -> bytes:
    hashed_string = hashlib.sha256(key.encode()).digest()

    fernet_key = base64.urlsafe_b64encode(hashed_string)

    cipher = Fernet(fernet_key)
    return cipher.decrypt(encripted)


def encrypt_json(data: dict, key: str) -> bytes:
    json_str = json.dumps(data)
    return encrypt(json_str.encode(), key)


def decrypt_json(encrypted_data: bytes, key: str) -> dict:
    decrypted = decrypt(encrypted_data, key)
    return json.loads(decrypted)
