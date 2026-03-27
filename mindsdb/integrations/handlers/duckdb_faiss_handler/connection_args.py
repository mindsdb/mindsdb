from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    persist_directory={
        "type": ARG_TYPE.STR,
        "description": "Optional custom directory for persisting data. If not provided, uses MindsDB's handler storage.",
        "required": False,
        "label": "Persist Directory",
    },
)

connection_args_example = OrderedDict(persist_directory="/tmp/data")
