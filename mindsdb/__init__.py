import os
import sys

from mindsdb.utilities.fs import get_or_create_dir_struct, create_directory, update_versions_file
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
for path in paths:
    create_directory(path)

os.environ['MINDSDB_STORAGE_PATH'] = paths['predictors']

from mindsdb_native import *
# Figure out how to add this as a module
import lightwood
#import dataskillet
import mindsdb.utilities.wizards as wizards
from mindsdb.interfaces.custom.model_interface import ModelInterface

from lightwood.__about__ import __version__ as lightwood_version
from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb_native.__about__ import __version__ as mindsdb_native_version

print('versions:')
print(f' - lightwood {lightwood_version}')
print(f' - MindsDB_native {mindsdb_native_version}')
print(f' - MindsDB {mindsdb_version}')

update_versions_file(
    mindsdb_config,
    {
        'lightwood': lightwood_version,
        'mindsdb_native': mindsdb_native_version,
        'mindsdb': mindsdb_version
    }
)
