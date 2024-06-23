from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Empress Embedded server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect to Empress Embedded server'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Empress Embedded server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Empress Embedded server.',
        'secret': True
    },
    server={
        'type': ARG_TYPE.STR,
        'description': 'The server name used to authenticate with the Empress Embedded server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specify database name to connect Empress Embedded server'
    },

)

connection_args_example = OrderedDict(
    host='localhost',
    port=6322,
    user='admin',
    password='password',
    server='test',
    database='test_db'
)
