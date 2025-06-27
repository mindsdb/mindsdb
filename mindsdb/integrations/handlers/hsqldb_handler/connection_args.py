from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    server_name={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the database'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect.'
    },
    database_name={
        'type': ARG_TYPE.STR,
        'description': '''
            The database name to use when connecting.
        '''
    },
    username={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate.',
        'secret': True
    },

)

connection_args_example = OrderedDict(
    server_name='localhost',
    port=9001,
    database_name='xdb',
    username='SA',
    password='password'
)
