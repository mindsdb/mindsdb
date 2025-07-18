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
    """split model name to parts

    Identifier may be:

    Examples:
        >>> resolve_model_identifier(['a', 'b'])
        ('a', 'b', None)

        >>> resolve_model_identifier(['a', '1'])
        (None, 'a', 1)

        >>> resolve_model_identifier(['a'])
        (None, 'a', None)

        >>> resolve_model_identifier(['a', 'b', 'c'])
        (None, None, None)  # not found

    Args:
        name (Identifier): Identifier parts

    Returns:
        tuple: (database_name, model_name, model_version)
    """
    parts = identifier.parts
    database_name = None
    model_name = None
    model_version = None

    parts_count = len(parts)
    if parts_count == 1:
        database_name = None
        model_name = parts[0]
        model_version = None
    elif parts_count == 2:
        if parts[-1].isdigit():
            database_name = None
            model_name = parts[0]
            model_version = int(parts[-1])
        else:
            database_name = parts[0]
            model_name = parts[1]
            model_version = None
    elif parts_count == 3:
        database_name = parts[0]
        model_name = parts[1]
        if parts[2].isdigit():
            model_version = int(parts[2])
        else:
            # not found
            return None, None, None

    return database_name, model_name, model_version


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
