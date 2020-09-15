import os
import sys

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
    config = Config(config_path)
except Exception as e:
    print(str(e))
    sys.exit(1)

paths = config.paths
create_directory(paths['datasources'])
create_directory(paths['predictors'])
create_directory(paths['static'])
create_directory(paths['tmp'])

os.environ['MINDSDB_STORAGE_PATH'] = paths['predictors']

from mindsdb_native import *
# Figure out how to add this as a module
import lightwood
#import dataskillet
import mindsdb.utilities.wizards as wizards
from mindsdb.interfaces.custom.model_interface import ModelInterface

from mindsdb_native.__about__ import __version__ as mindsdb_native_version
print(f'lightwood version {lightwood.__version__}')
print(f'MindsDB_native version {mindsdb_native_version}')
