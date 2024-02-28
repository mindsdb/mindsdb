from collections import OrderedDict

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb_sql import parse_sql
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

logger = log.getLogger(__name__)

class KDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the KDB statements.
    """
    
    name = 'kdb'
    
    def __init__(self, name=None, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        pass