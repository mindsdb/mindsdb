from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the TDEngine server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the TDEngine server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the TDEngine server.'
    },
    url={
        'type': ARG_TYPE.STR,
        'description': 'The url of the TDEngine server Instance. '
    },
    token={
        'type': ARG_TYPE.INT,
        'description': 'Unique Token to COnnect TDEngine'
    },
)

connection_args_example = OrderedDict(
    url='127.0.0.1:6041',
    token='<token comes here>',
    user='root',
    password='taosdata',
    database='test'
)
