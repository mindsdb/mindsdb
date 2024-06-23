from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Planetscale server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Planetscale server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Planetscale server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Planetscale server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Planetscale server. Must be an integer.'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=4000,
    user='root',
    password='password',
    database='database'
)
