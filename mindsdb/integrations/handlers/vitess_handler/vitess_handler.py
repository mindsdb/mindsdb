from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from collections import OrderedDict


from ..mysql_handler import Handler as MysqlHandler


class VitessHandler(MysqlHandler):
    """
    This handler handles connection and execution of the Vitess statements.
    """
    name = 'vitess'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)



connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Vitess server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Vitess server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Vitess server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Vitess server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Vitess server. Must be an integer.'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=5432,
    user='root',
    password='<???>',
    database='database'
)
