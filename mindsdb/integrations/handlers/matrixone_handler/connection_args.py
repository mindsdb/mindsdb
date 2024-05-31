from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the MatrixOne server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the MatrixOne server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the MatrixOne server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the MatrixOne server. '
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the MatrixOne server. Must be an integer.'
    },
    ssl={
        'type': ARG_TYPE.BOOL,
        'description': 'Set it to False to disable ssl.'
    },
    ssl_ca={
        'type': ARG_TYPE.PATH,
        'description': 'Path or URL of the Certificate Authority (CA) certificate file'
    },
    ssl_cert={
        'type': ARG_TYPE.PATH,
        'description': 'Path name or URL of the server public key certificate file'
    },
    ssl_key={
        'type': ARG_TYPE.PATH,
        'description': 'The path name or URL of the server private key file'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=6001,
    user='dump',
    password='111',
    database='mo_catalog'
)
