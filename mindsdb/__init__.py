import os
import sys

from mindsdb.utilities.fs import get_or_create_dir_struct, create_directory
from mindsdb.utilities.wizards import cli_config
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import args_parse

config_dir, predictor_dir, datasource_dir = get_or_create_dir_struct()

config_path = os.path.join(config_dir, 'config.json')
if not os.path.exists(config_path):
    _ = cli_config(None, None, predictor_dir, datasource_dir, config_dir, use_default=True)

args = args_parse()
if args.config is not None:
    config_path = args.config

try:
    config = Config(config_path)
except Exception as e:
    print(str(e))
    sys.exit(1)

try:
    datasource_dir = config['interface']['datastore']['storage_dir']
except KeyError:
    pass
try:
    predictor_dir = config['interface']['mindsdb_native']['storage_dir']
except KeyError:
    pass

create_directory(datasource_dir)
create_directory(predictor_dir)

temp_path = os.path.join(predictor_dir, 'tmp')
create_directory(temp_path)

os.environ['MINDSDB_STORAGE_PATH'] = predictor_dir
os.environ['MINDSDB_TEMP_PATH'] = temp_path

from mindsdb_native import *
# Figure out how to add this as a module
import lightwood
#import dataskillet
import mindsdb.utilities.wizards as wizards
