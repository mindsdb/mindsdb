from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    url={
        "type": ARG_TYPE.STR,
        "description": "The URL of the Upstash Vector REST API. (Accesable from the Upstash Console)",
        "required": True,
        "secret": True
    },
    token={
        "type": ARG_TYPE.STR,
        "description": "The token of the Upstash Vector REST API. (Accesable from the Upstash Console)",
        "required": True,
    },
    retries={
        "type": ARG_TYPE.INT,
        "description": "The number of retries to make before giving up. (default=3)",
        "required": False,
    },
    retry_interval={
        "type": ARG_TYPE.FLOAT,
        "description": "The number of seconds to wait between retries. (default=1.0)",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    url="UPSTASH_VECTOR_REST_URL",
    token="UPSTASH_VECTOR_REST_TOKEN",
    retries=5,
    retry_interval=2.0,
)
