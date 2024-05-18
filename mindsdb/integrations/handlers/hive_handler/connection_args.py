from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Hive server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Hive server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Hive server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Hive server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Hive server. Must be an integer.'
    },
    auth={
        'type': ARG_TYPE.STR,
        'description': 'The Auth type of the Hive server.'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port='10000',
    auth='CUSTOM',
    user='root',
    password='password',
    database='database'
)
