from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from collections import OrderedDict


from ..mysql_handler import Handler as MysqlHandler


class OceanBaseHandler(MysqlHandler):
    """
    This handler handles connection and execution of the OceanBase statements.
    """
    name = 'oceanbase'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)



connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the OceanBase server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the OceanBase server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the OceanBase server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the OceanBase server. '
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the OceanBase server. Must be an integer.'
    }
)

connection_args_example = OrderedDict(
    host='localhost',
    port=9030,
    user='root',
    password='',
    database='test'
)
