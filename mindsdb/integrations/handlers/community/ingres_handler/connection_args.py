from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Ingres server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Ingres server.',
        'secret': True
    },
    server={
        'type': ARG_TYPE.STR,
        'description': 'The server used to authenticate with the Ingres server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specify database name to connect Ingres server'
    },
    servertype={
        'type': ARG_TYPE.STR,
        'description': 'Specify server type to connect Ingres server'
    }
)

connection_args_example = OrderedDict(
    user='admin',
    password='password',
    server='(local)',
    database='test_db',
    servertype='ingres'
)
