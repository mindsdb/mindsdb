from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from collections import OrderedDict


from ..yugabyte_handler import Handler as YugaByteHandler


class MaterializeHandler(YugaByteHandler):
    """
    This handler handles connection and execution of the Materialize statements.
    """
    name = 'materialize'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)






connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Materialize server/database.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Materialize server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Materialize server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect Materialize server'
    }, 
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specify database name  to connect Materialize server'
    },


)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=6875,
    password='',
    user='USER',
    database='materialize'
    

)
