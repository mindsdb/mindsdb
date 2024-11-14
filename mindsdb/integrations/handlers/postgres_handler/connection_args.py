from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the PostgreSQL server.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the PostgreSQL server.',
        'required': True,
        'label': 'Password',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the PostgreSQL server.',
        'required': True,
        'label': 'Database'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the PostgreSQL server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the PostgreSQL server. Must be an integer.',
        'required': True,
        'label': 'Port'
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': 'The schema in which objects are searched first.',
        'required': False,
        'label': 'Schema'
    },
    sslmode={
        'type': ARG_TYPE.STR,
        'description': 'sslmode that will be used for connection.',
        'required': False,
        'label': 'sslmode'
    },
    connection_parameters={
        'type': ARG_TYPE.DICT,
        'description': 'Connection string parameters',
        'required': False,
        'label': 'connection_parameters'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=5432,
    user='root',
    schema='public',
    password='password',
    database='database'
)
