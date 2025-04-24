import os
import time
import json
from pathlib import Path


import pytest
import pandas as pd

from mindsdb.utilities import log


logger = log.getLogger(__name__)

USE_PERSISTENT_STORAGE = bool(int(os.getenv('USE_PERSISTENT_STORAGE') or "0"))
TEST_CONFIG = os.path.dirname(os.path.realpath(__file__)) + '/config/config.json'
TEMP_DIR = Path(__file__).parent.absolute().joinpath('../../').joinpath(
    f'temp/test_storage_{int(time.time()*1000)}/' if not USE_PERSISTENT_STORAGE else 'temp/test_storage/'
).resolve()
TEMP_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = TEMP_DIR.joinpath('config.json')


# Makes pytest print the name of parameters for parametrized tests
def pytest_make_parametrize_id(config, val, argname):
    return f"{argname}={val}"


def make_test_csv(name, data):
    test_csv_path = TEMP_DIR.joinpath(f'{name}.csv').resolve()
    df = pd.DataFrame(data)
    df.to_csv(test_csv_path, index=False)
    return str(test_csv_path)


@pytest.fixture(scope="session")
def temp_dir():
    """Create temp directory to store mindsdb data inside it.
    The directory is created once for the whole test session.
    See 'scope' fixture parameter
    """
    temp_dir = Path(__file__).parent.absolute().joinpath('../../').joinpath(
        f'temp/test_storage_{int(time.time()*1000)}/' if not USE_PERSISTENT_STORAGE else 'temp/test_storage/'
    ).resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


@pytest.fixture(scope="module")
def config(temp_dir):
    """Create config used by mindsdb app in tests.
    The config is created once for the whole test module.
    See 'scope' fixture parameter.
    """
    with open(TEST_CONFIG, 'rt') as f:
        config_json = json.loads(f.read())
        config_json['paths']['root'] = f'{TEMP_DIR}'
        config_json['storage_db'] = f'sqlite:///{TEMP_DIR}/mindsdb.sqlite3.db?check_same_thread=False&timeout=30'
        config_json['integrations'] = {}

    return config_json


def override_recursive(a, b):
    """Overrides some elements in json 'a' by elements in json 'b'"""
    for key in b:
        if isinstance(b[key], dict) is False:
            a[key] = b[key]
        elif key not in a or isinstance(a[key], dict) is False:
            a[key] = b[key]
        # make config section empty by demand
        elif isinstance(b[key], dict) is True and b[key] == {}:
            a[key] = b[key]
        else:
            override_recursive(a[key], b[key])
