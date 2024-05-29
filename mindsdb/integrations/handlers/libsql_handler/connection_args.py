from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    database={
        "type": ARG_TYPE.STR,
        "description": "The database file where the data will be stored.",
    },
    sync_url={
        "type": ARG_TYPE.STR,
        "description": "The database URL where the data is synced with.",
    },
    auth_token={
        "type": ARG_TYPE.STR,
        "description": "The JWT auth token to authenticate with the sync database.",
        "secret": True
    },
)

connection_args_example = OrderedDict(database="chinook.db")
