import os
import sys

from mindsdb.__about__ import __package_name__ as name, __version__
from mindsdb.interfaces.state.config import Config
from mindsdb.utilities.functions import args_parse, is_notebook


try:
    if not is_notebook():
        args = args_parse()
        config_path = args.config
    else:
        config_path = None
except:
    # This fials in some notebooks
    config_path = None

mindsdb_config = Config(config_path)


os.environ['MINDSDB_STORAGE_PATH'] = mindsdb_config['paths']['predictors']
os.environ['DEFAULT_LOG_LEVEL'] = os.environ.get('DEFAULT_LOG_LEVEL', 'ERROR')
os.environ['LIGHTWOOD_LOG_LEVEL'] = os.environ.get('LIGHTWOOD_LOG_LEVEL', 'ERROR')

from mindsdb_native import *
# Figure out how to add this as a module
import lightwood
#import dataskillet
import mindsdb.utilities.wizards as wizards
from mindsdb.interfaces.custom.model_interface import ModelInterface
