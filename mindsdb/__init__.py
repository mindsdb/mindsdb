import sys
if sys.version_info < (3,3):
    sys.exit('Sorry, For MindsDB Python < 3.3 is not supported')


from .libs.data_types.data_source import DataSource
from .libs.data_sources import *
from .libs.controllers.mindsdb_controller import MindsDBController

name = "mindsdb"
MindsDB = MindsDBController
MDS = DataSource # A Mindsdb Data Source