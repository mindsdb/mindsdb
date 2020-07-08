import os
import json

from mindsdb.utilities.fs import get_or_create_dir_struct
from mindsdb.utilities.wizards import cli_config

config_dir, predictor_dir, datasource_dir = get_or_create_dir_struct()
config_path = os.path.join(config_dir,'config.json')
if not os.path.exists(config_path):
    if 'DEV_CONFIG_PATH' in os.environ:
        config_dir = os.environ['DEV_CONFIG_PATH']
    _ = cli_config(None,None,predictor_dir,datasource_dir,config_dir,use_default=True)

with open(config_path, 'r') as fp:
    os.environ['MINDSDB_STORAGE_PATH'] = json.load(fp)['interface']['mindsdb_native']['storage_dir']


from mindsdb_native import *
# Figure out how to add this as a module
import lightwood
#import dataskillet
import mindsdb.utilities.wizards as wizards
