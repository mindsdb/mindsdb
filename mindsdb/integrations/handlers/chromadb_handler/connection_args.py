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
    api_key={
        "type": ARG_TYPE.STR,
        "description": "API key for ChromaDB authentication",
        "required": False,
        "secret": True,
    },
    tenant={
        "type": ARG_TYPE.STR,
        "description": "ChromaDB tenant (required if api_key is provided)",
        "required": False,
    },
    database={
        "type": ARG_TYPE.STR,
        "description": "ChromaDB database (required if api_key is provided)",
        "required": False,
    },
    ssl={
        "type": ARG_TYPE.BOOL,
        "description": "Use SSL for ChromaDB connection",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    host="localhost",
    port="8000",
    persist_directory="chromadb",
    api_key="your_api_key_here",
    tenant="default_tenant",
    database="default_database",
    ssl=False,
)
