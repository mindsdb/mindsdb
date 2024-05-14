from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    weaviate_url={
        "type": ARG_TYPE.STR,
        "description": "weaviate url/ local endpoint",
        "required": False,
    },
    weaviate_api_key={
        "type": ARG_TYPE.STR,
        "description": "weaviate API KEY",
        "required": False,
        "secret": True
    },
    persistence_directory={
        "type": ARG_TYPE.STR,
        "description": "persistence directory for weaviate",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    weaviate_url="http://localhost:8080",
    weaviate_api_key="<api_key>",
    persistence_directory="db_path",
)
