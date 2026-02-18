from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Impala server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Impala server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Impala server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Impala server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Impala server. Must be an integer. Default is 21050'
    }

)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=21050,
    user='USERNAME',
    password='P@55W0Rd',
    database='D4t4bA5e'
)
