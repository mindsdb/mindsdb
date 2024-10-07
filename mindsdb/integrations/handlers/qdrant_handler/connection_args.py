from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    location={
        "type": ARG_TYPE.STR,
        "description": "If `:memory:` - use in-memory Qdrant instance. If a remote URL - connect to a remote Qdrant instance. Example: `http://localhost:6333`",
        "required": False,
    },
    url={
        "type": ARG_TYPE.STR,
        "description": "URL of Qdrant service. Either host or a string of type [scheme]<host><[port][prefix]. Ex: http://localhost:6333/service/v1",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "Host name of Qdrant service. The port and host are used to construct the connection URL.",
        "required": False,
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "Port of the REST API interface. Default: 6333",
        "required": False,
    },
    grpc_port={
        "type": ARG_TYPE.INT,
        "description": "Port of the gRPC interface. Default: 6334",
        "required": False,
    },
    prefer_grpc={
        "type": ARG_TYPE.BOOL,
        "description": "If `true` - use gPRC interface whenever possible in custom methods. Default: false",
        "required": False,
    },
    https={
        "type": ARG_TYPE.BOOL,
        "description": "If `true` - use https protocol.",
        "required": False,
    },
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "API key for authentication in Qdrant Cloud.",
        "required": False,
        "secret": True
    },
    prefix={
        "type": ARG_TYPE.STR,
        "description": "If set, the value is added to the REST URL path. Example: `service/v1` will result in `http://localhost:6333/service/v1/{qdrant-endpoint}` for REST API",
        "required": False,
    },
    timeout={
        "type": ARG_TYPE.INT,
        "description": "Timeout for REST and gRPC API requests. Defaults to 5.0 seconds for REST and unlimited for gRPC",
        "required": False,
    },
    path={
        "type": ARG_TYPE.STR,
        "description": "Persistence path for a local Qdrant instance.",
        "required": False,
    },
    collection_config={
        "type": ARG_TYPE.DICT,
        "description": "Collection creation configuration. See https://qdrant.github.io/qdrant/redoc/index.html#tag/collections/operation/create_collection",
        "required": True,
    },
)

connection_args_example = {
    "location": ":memory:",
    "collection_config": {
        "size": 386,
        "distance": "Cosine"
    }
}
