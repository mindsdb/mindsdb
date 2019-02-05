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


CONFIG.DEFAULT_LOG_LEVEL

