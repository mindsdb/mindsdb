from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from collections import OrderedDict


from ..mysql_handler import Handler as MySQLHandler, connection_args, connection_args_example


class RocksetIntegration(MysqlHandler):
    """
    This handler handles connection and execution of the OceanBase statements.
    """
    name = 'rockset'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
