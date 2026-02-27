from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        "type": ARG_TYPE.STR,
        "description": "chromadb server host",
        "required": False,
    },
    port={
        "type": ARG_TYPE.STR,
        "description": "chromadb server port",
        "required": False,
    },
    persist_directory={
        "type": ARG_TYPE.STR,
        "description": "persistence directory for ChromaDB",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    host="localhost",
    port="8000",
    persist_directory="chromadb",
)
