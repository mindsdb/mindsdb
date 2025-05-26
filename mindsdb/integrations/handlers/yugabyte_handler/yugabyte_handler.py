from collections import OrderedDict

from mindsdb.integrations.handlers.postgres_handler.postgres_handler import (
    PostgresHandler,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class YugabyteHandler(PostgresHandler):
    """
    This handler handles connection and execution of the YugabyteSQL statements.
    """

    name = 'yugabyte'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the YugabyteDB server/database.',
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the YugabyteDB server.',
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the YugabyteDB server.',
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect YugabyteDB server',
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specify database name  to connect YugabyteDB server',
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': '(OPTIONAL) comma seperated value of schema to be considered while querying',
    },
    sslmode={
        'type': ARG_TYPE.STR,
        'description': ''' (OPTIONAL) Specifies the SSL mode for the connection, determining whether to use SSL encryption and the level of verification required
        (e.g., "**disable**", "**allow**", "**prefer**", "**require**", "**verify-ca**", "**verify-full**")''',
    },
)

connection_args_example = OrderedDict(
    host='127.0.0.1', port=5433, password='', user='admin', database='yugabyte'
)
