from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from ..scylla_handler import Handler as ScyllaHandler


class CassandraHandler(ScyllaHandler):
    """
    This handler handles connection and execution of the Cassandra statements.
    """
    name = 'cassandra'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'User name'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'Password'
    },
    protocol_version={
        'type': ARG_TYPE.INT,
        'description': 'The protocol version.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'Server host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Server port'
    },
    keyspace={
        'type': ARG_TYPE.STR,
        'description': 'Name of keyspace'
    },
    secure_connect_bundle={
        'type': ARG_TYPE.PATH,
        'description': 'Path or URL to the secure connect bundle'
    }
)
