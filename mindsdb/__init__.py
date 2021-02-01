import os
import sys
import json

from mindsdb.__about__ import __package_name__ as name, __version__   # noqa
from mindsdb.utilities.fs import get_or_create_dir_struct, create_dirs_recursive
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import args_parse, is_notebook
from mindsdb.__about__ import __version__ as mindsdb_version

try:
    if not is_notebook():
        args = args_parse()
except:
    # This fials in some notebooks ... check above for is_notebook is still needed because even if the exception is caught trying to read the arg still leads to failure in other notebooks... notebooks a
    args = None

# ---- CHECK SYSTEM ----
if not (sys.version_info[0] >= 3 and sys.version_info[1] >= 6):
    print("""
MindsDB server requires Python >= 3.6 to run

Once you have Python 3.6 installed you can tun mindsdb as follows:

1. create and activate venv:
python3.6 -m venv venv
source venv/bin/activate

2. install MindsDB:
pip3 install mindsdb

3. Run MindsDB
python3.6 -m mindsdb

More instructions in https://docs.mindsdb.com
    """)
    exit(1)

# --- VERSION MODE ----
if args is not None and args.version:
    print(f'MindsDB {mindsdb_version}')
    sys.exit(0)

# --- MODULE OR LIBRARY IMPORT MODE ----

if args is not None and args.config is not None:
    config_path = args.config
else:
    config_dir, storage_dir = get_or_create_dir_struct()
    config_path = os.path.join(config_dir, 'config.json')
    if not os.path.isfile(config_path):
        with open(config_path, 'w') as fp:
            json.dump({'config_version': '1.4', 'storage_dir': storage_dir, 'api': {}}, fp)

mindsdb_config = Config(config_path)
create_dirs_recursive(mindsdb_config.paths)

os.environ['DEFAULT_LOG_LEVEL'] = os.environ.get('DEFAULT_LOG_LEVEL', 'ERROR')
os.environ['LIGHTWOOD_LOG_LEVEL'] = os.environ.get('LIGHTWOOD_LOG_LEVEL', 'ERROR')
os.environ['MINDSDB_CONFIG_PATH'] = config_path
os.environ['MINDSDB_STORAGE_PATH'] = mindsdb_config.paths['predictors']

from mindsdb_native import *
# Figure out how to add this as a module
import lightwood
#import dataskillet
import mindsdb.utilities.wizards as wizards
from mindsdb.interfaces.custom.model_interface import ModelInterface
