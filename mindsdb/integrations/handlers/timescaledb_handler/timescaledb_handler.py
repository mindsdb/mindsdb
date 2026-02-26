from mindsdb.integrations.handlers.postgres_handler import Handler as PostgresHandler
from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class TimeScaleDBHandler(PostgresHandler):
    name = 'timescaledb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the TimeScaleDB server/database.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': """
            The database name to use when connecting with the TimeScaleDB server.
        """
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the TimeScaleDB server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the TimeScaleDB server.'
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': 'The schema in which objects are searched first.',
        'required': False,
        'label': 'Schema'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect TimeScaleDB '
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=5432,
    password='password',
    user='root',
    database="timescaledb",
    schema='public'
)
