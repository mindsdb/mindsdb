import argparse
import datetime
from functools import wraps

import requests

from mindsdb.utilities.fs import create_process_mark, delete_process_mark


def args_parse():
    parser = argparse.ArgumentParser(description='CL argument for mindsdb server')
    parser.add_argument('--api', type=str, default=None)
    parser.add_argument('--config', type=str, default=None)
    parser.add_argument('--install-handlers', type=str, default=None)
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--no_studio', action='store_true')
    parser.add_argument('-v', '--version', action='store_true')
    parser.add_argument('--ray', action='store_true', default=None)
    return parser.parse_args()


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
                print(f'cast {row[key]} to {int(row[key])}')
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


def mark_process(name):
    def mark_process_wrapper(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            mark = create_process_mark(name)
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
            print(f'Is no connection. {e}')
            raise
        except Exception as e:
            print(f'Is something wrong with getting version_for_updating_predictors.txt: {e}')
            raise

        if res.status_code != 200:
            print(f'Cant get version_for_updating_predictors.txt: returned status code = {res.status_code}')
            raise

        try:
            versions_for_updating_predictors = res.text.replace(' \t\r', '').split('\n')
        except Exception as e:
            print(f'Cant decode version_for_updating_predictors.txt: {e}')
            raise
    except Exception:
        return False, versions_for_updating_predictors

    versions_for_updating_predictors = [x for x in versions_for_updating_predictors if len(x) > 0]
    return True, versions_for_updating_predictors
