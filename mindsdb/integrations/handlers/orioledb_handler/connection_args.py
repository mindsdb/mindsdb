from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the OrioleDB server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the OrioleDB server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the OrioleDB server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the OrioleDB server. '
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the OrioleDB server. Must be an integer.'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=5432,
    user='postgres',
    password='<???>',
    database='database'
)
