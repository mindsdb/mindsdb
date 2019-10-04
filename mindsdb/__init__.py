import sys
if sys.version_info < (3,6):
    sys.exit('Sorry, For MindsDB Python < 3.6 is not supported')

from mindsdb.config import CONFIG
import mindsdb.libs.constants.mindsdb as CONST

from mindsdb.__about__ import __package_name__ as name, __version__
from mindsdb.libs.controllers.predictor import Predictor

# Data Sources
from mindsdb.libs.data_sources.file_ds import FileDS
from mindsdb.libs.data_sources.s3_ds import S3DS
from mindsdb.libs.data_sources.mysql_ds import MySqlDS

MindsDB = Predictor
