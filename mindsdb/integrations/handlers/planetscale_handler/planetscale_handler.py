from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler


class PlanetScaleHandler(MySQLHandler):
    """
    This handler handles the connection and execution of queries against PlanetScale.
    """
    name = 'planet_scale'
    
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
