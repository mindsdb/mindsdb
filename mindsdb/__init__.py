import os
import sys

from mindsdb.__about__ import __package_name__ as name, __version__   # noqa
from mindsdb.utilities.fs import get_or_create_dir_struct, create_directory
from mindsdb.utilities.wizards import cli_config
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import args_parse

config_dir, storage_dir = get_or_create_dir_struct()

config_path = os.path.join(config_dir, 'config.json')
if not os.path.exists(config_path):
    _ = cli_config(None, None, storage_dir, config_dir, use_default=True)

args = args_parse()
if args.config is not None:
    config_path = args.config

try:
    mindsdb_config = Config(config_path)
except Exception as e:
    print(str(e))
    sys.exit(1)

paths = mindsdb_config.paths
for path in paths.values():
    create_directory(path)

os.environ['MINDSDB_STORAGE_PATH'] = paths['predictors']
os.environ['DEFAULT_LOG_LEVEL'] = os.environ.get('DEFAULT_LOG_LEVEL', 'ERROR')
os.environ['LIGHTWOOD_LOG_LEVEL'] = os.environ.get('LIGHTWOOD_LOG_LEVEL', 'ERROR')

from mindsdb_native import *
# Figure out how to add this as a module
import lightwood
#import dataskillet
import mindsdb.utilities.wizards as wizards
from mindsdb.interfaces.custom.model_interface import ModelInterface
