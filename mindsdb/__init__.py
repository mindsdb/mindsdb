import os
import sys
import json

from mindsdb.__about__ import __package_name__ as name, __version__   # noqa
from mindsdb.utilities.fs import get_or_create_dir_struct, create_dirs_recursive
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import args_parse, is_notebook


config_dir, storage_dir = get_or_create_dir_struct()
config_path = os.path.join(config_dir, 'config.json')

try:
    if not is_notebook():
        args = args_parse()
        if args.config is not None:
            config_path = args.config
except:
    # This fials in some notebooks ... check above for is_notebook is still needed because even if the exception is caught trying to read the arg still leads to failure in other notebooks... notebooks a
    pass

if not os.path.isfile(config_path):
    with open(config_path, 'w') as fp:
        json.dump({'config_version': '1.4', 'storage_dir': storage_dir, 'api': {}}, fp)

mindsdb_config = Config(config_path)


paths = mindsdb_config.paths
create_dirs_recursive(paths)

os.environ['MINDSDB_STORAGE_PATH'] = paths['predictors']
os.environ['DEFAULT_LOG_LEVEL'] = os.environ.get('DEFAULT_LOG_LEVEL', 'ERROR')
os.environ['LIGHTWOOD_LOG_LEVEL'] = os.environ.get('LIGHTWOOD_LOG_LEVEL', 'ERROR')

from mindsdb_native import *
# Figure out how to add this as a module
import lightwood
#import dataskillet
import mindsdb.utilities.wizards as wizards
from mindsdb.interfaces.custom.model_interface import ModelInterface
