from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    username={
        'type': ARG_TYPE.STR,
        'description': 'The username used to authenticate with the MongoDB server.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the MongoDB server.',
        'required': True,
        'label': 'Password',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the MongoDB server.',
        'required': False,
        'label': 'Database'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the MongoDB server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the MongoDB server. Must be an integer.',
        'required': True,
        'label': 'Port'
    },
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=27017,
    username='mongo',
    password='password',
    database='database'
)
