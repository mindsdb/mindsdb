from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    persist_directory={
        "type": ARG_TYPE.STR,
        "description": "The uri of the database.",
        "required": True,
    },
    api_key={
        "type": ARG_TYPE.STR,
        "description": "If presented, connect to LanceDB cloud. Otherwise, connect to a database on file system or cloud storage.",
        "required": False,
        "secret": True
    },
    region={
        "type": ARG_TYPE.STR,
        "description": "The region to use for LanceDB Cloud.",
        "required": False,
    },
    host_override={
        "type": ARG_TYPE.STR,
        "description": "The override url for LanceDB Cloud.",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    persist_directory="~/lancedb",
    api_key=None,
    region="us-west-2",
    host_override=None,
)
