import argparse
import datetime
from functools import wraps
import hashlib
import base64
import os
import textwrap
from cryptography.fernet import Fernet
from collections.abc import Callable

import requests
from mindsdb_sql import get_lexer_parser
from mindsdb_sql.parser.ast import Identifier

from mindsdb.utilities.fs import create_process_mark, delete_process_mark, set_process_mark
from mindsdb.utilities import log
from mindsdb.utilities.config import Config


logger = log.getLogger(__name__)


def args_parse():
    parser = argparse.ArgumentParser(description='CL argument for mindsdb server')
    parser.add_argument('--api', type=str, default=None)
    parser.add_argument('--config', type=str, default=None)
    parser.add_argument('--install-handlers', type=str, default=None)
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--no_studio', action='store_true')
    parser.add_argument('-v', '--version', action='store_true')
    parser.add_argument('--ml_task_queue_consumer', action='store_true', default=None)
    return parser.parse_args()


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
    '''
    '''
    keys = [x for x in row.keys() if x in field_types]
    for key in keys:
        t = field_types[key]
        if t == 'Timestamp' and isinstance(row[key], (int, float)):
            timestamp = datetime.datetime.utcfromtimestamp(row[key])
            row[key] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        elif t == 'Date' and isinstance(row[key], (int, float)):
            timestamp = datetime.datetime.utcfromtimestamp(row[key])
            row[key] = timestamp.strftime('%Y-%m-%d')
        elif t == 'Int' and isinstance(row[key], (int, float, str)):
            try:
                logger.debug(f'cast {row[key]} to {int(row[key])}')
                row[key] = int(row[key])
            except Exception:
                pass


def is_notebook():
    try:
        if 'IPKernelApp' in get_ipython().config:
            return True
        else:
            return False
    except NameError:
        return False      # Probably standard Python interpreter


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


def get_versions_where_predictors_become_obsolete():
    """ Get list of MindsDB versions in which predictors should be retrained
        Returns:
            list of str or False
    """
    versions_for_updating_predictors = []
    try:
        try:
            res = requests.get(
                'https://mindsdb-cloud-public-service-files.s3.us-east-2.amazonaws.com/version_for_updating_predictors.txt',
                timeout=0.5
            )
        except (ConnectionError, requests.exceptions.ConnectionError) as e:
            logger.error(f'Is no connection. {e}')
            raise
        except Exception as e:
            logger.error(f'Is something wrong with getting version_for_updating_predictors.txt: {e}')
            raise

        if res.status_code != 200:
            logger.error(f'Cant get version_for_updating_predictors.txt: returned status code = {res.status_code}')
            raise

        try:
            versions_for_updating_predictors = res.text.replace(' \t\r', '').split('\n')
        except Exception as e:
            logger.error(f'Cant decode version_for_updating_predictors.txt: {e}')
            raise
    except Exception:
        return False, versions_for_updating_predictors

    versions_for_updating_predictors = [x for x in versions_for_updating_predictors if len(x) > 0]
    return True, versions_for_updating_predictors


def init_lexer_parsers():
    get_lexer_parser('mindsdb')
    get_lexer_parser('mysql')


def resolve_table_identifier(identifier: Identifier, default_database: str = None) -> tuple:
    parts = identifier.parts

    parts_count = len(parts)
    if parts_count == 1:
        return (None, parts[0])
    elif parts_count == 2:
        return (parts[0], parts[1])
    else:
        raise Exception(f'Table identifier must contain max 2 parts: {parts}')


def resolve_model_identifier(identifier: Identifier) -> tuple:
    """ split model name to parts

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
