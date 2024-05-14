from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the EdgelessDB server.',
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the EdgelessDB server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the EdgelessDB server.',
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the EdgelessDB server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the EdgelessDB server. Must be an integer.',
    },
    ssl={'type': ARG_TYPE.BOOL, 'description': 'Set it to False to disable ssl.'},
    ssl_ca={
        'type': ARG_TYPE.PATH,
        'description': 'Path or URL of the Certificate Authority (CA) certificate file',
    },
    ssl_cert={
        'type': ARG_TYPE.PATH,
        'description': 'Path name or URL of the server public key certificate file',
    },
    ssl_key={
        'type': ARG_TYPE.PATH,
        'description': 'The path name or URL of the server private key file',
    },
)

connection_args_example = OrderedDict(
    host='127.0.0.1', port=3306, user='root', password='password', database='database'
)
