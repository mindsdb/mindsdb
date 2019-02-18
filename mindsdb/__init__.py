import sys
if sys.version_info < (3,3):
    sys.exit('Sorry, For MindsDB Python < 3.3 is not supported')

from mindsdb.config import CONFIG
import mindsdb.libs.constants.mindsdb as CONST

from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_sources import *
from mindsdb.libs.controllers.predictor import Predictor

name = "mindsdb"
MindsDB = Predictor
MDS = DataSource # A Mindsdb Data Source
