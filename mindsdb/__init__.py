import sys
if sys.version_info < (3,3):
    sys.exit('Sorry, For MindsDB Python < 3.3 is not supported')

from mindsdb.config import CONFIG
import mindsdb.libs.constants.mindsdb as CONST

from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_sources import *
from mindsdb.libs.controllers.mindsdb_controller import MindsDBController

name = "mindsdb"
MindsDB = MindsDBController
MDS = DataSource # A Mindsdb Data Source


class Anon:
    pass
config_vars = Anon()

config_vars.__dict__['DEBUG_LOG_LEVEL'] = CONFIG.DEBUG_LOG_LEVEL
config_vars.__dict__['DEFAULT_LOG_LEVEL'] = CONFIG.DEFAULT_LOG_LEVEL
config_vars.__dict__['WARNING_LOG_LEVEL'] = CONFIG.WARNING_LOG_LEVEL
config_vars.__dict__['ERROR_LOG_LEVEL'] = CONFIG.ERROR_LOG_LEVEL
config_vars.__dict__['NO_LOGS_LOG_LEVEL'] = CONFIG.NO_LOGS_LOG_LEVEL 
