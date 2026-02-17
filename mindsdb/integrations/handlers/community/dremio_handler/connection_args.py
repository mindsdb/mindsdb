from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Dremio server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The port that Dremio is running on.'
    },
    username={
        'type': ARG_TYPE.STR,
        'description': 'The username used to authenticate with the Dremio server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Dremio server.',
        'secret': True
    }
)

connection_args_example = OrderedDict(
    host='localhost',
    database=9047,
    username='admin',
    password='password'
)
