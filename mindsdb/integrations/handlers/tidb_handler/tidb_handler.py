from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from ..mysql_handler import Handler as MySQLHandler


class TiDBHandler(MySQLHandler):
    """
    This handler handles connection and execution of the TiDB statements.
    """
    name = 'tidb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the TiDB server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the TiDB server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the TiDB server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the TiDB server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the TiDB server. Must be an integer.'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=4000,
    user='root',
    password='password',
    database='database'
)
