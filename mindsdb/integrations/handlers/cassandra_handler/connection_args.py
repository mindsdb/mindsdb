from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'User name',
        'required': True,
        'label': 'User',
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'Password',
        'required': True,
        'label': 'Password',
        'secret': True
    },
    protocol_version={
        'type': ARG_TYPE.INT,
        'description': 'is not required and defaults to 4.',
        'required': False,
        'label': 'Protocol version',
    },
    host={
        'type': ARG_TYPE.STR,
        'description': ' is the host name or IP address of the Cassandra database.',
        'required': True,
        'label': 'Host',
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Server port',
        'required': True,
        'label': 'Port',
    },
    keyspace={
        'type': ARG_TYPE.STR,
        'description': ' is the keyspace to connect, the top level container for tables.',
        'required': True,
        'label': 'Keyspace',
    },
    secure_connect_bundle={
        'type': ARG_TYPE.STR,
        'description': 'Path or URL to the secure connect bundle',
        'required': False,
        'label': 'Host',
    },
)
