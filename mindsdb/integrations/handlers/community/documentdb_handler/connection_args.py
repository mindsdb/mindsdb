from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the DocumentDB server.',
        'required': True,
        'label': 'User',
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the DocumentDB server.',
        'required': True,
        'label': 'Password',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the DocumentDB server.',
        'required': True,
        'label': 'Database',
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the DocumentDB server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host',
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the DocumentDB server. Must be an integer.',
        'required': True,
        'label': 'Port',
    },
    kwargs={
        'type': ARG_TYPE.DICT,
        'description': 'Additional parameters of DocumentDB same as MongoDB.',
        'required': False,
        'label': 'Kwargs',
    },
)


connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=27017,
    username='documentdb',
    password='password',
    database='database',
)
