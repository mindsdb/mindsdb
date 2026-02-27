from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database file to read and write from. The special value :memory: (default) can be used to create an in-memory database.',
    },
    read_only={
        'type': ARG_TYPE.BOOL,
        'description': 'A flag that specifies if the connection should be made in read-only mode.',
    },
)

connection_args_example = OrderedDict(database='db.duckdb', read_only=True)
