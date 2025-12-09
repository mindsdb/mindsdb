from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    prometheus_url={
        "type": ARG_TYPE.STR,
        "description": "The base URL of the Prometheus server (e.g., http://localhost:9090)",
        "required": True,
        "label": "Prometheus URL",
    },
    username={
        "type": ARG_TYPE.STR,
        "description": "Username for basic authentication (optional)",
        "required": False,
        "label": "Username",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "Password for basic authentication (optional, required if username is provided)",
        "secret": True,
        "required": False,
        "label": "Password",
    },
    bearer_token={
        "type": ARG_TYPE.PWD,
        "description": "Bearer token for token-based authentication (optional, alternative to username/password)",
        "secret": True,
        "required": False,
        "label": "Bearer Token",
    },
    timeout={
        "type": ARG_TYPE.INT,
        "description": "Request timeout in seconds (default: 10)",
        "required": False,
        "label": "Timeout",
    },
)

connection_args_example = OrderedDict(
    prometheus_url="http://localhost:9090",
    username="admin",
    password="secret",
    timeout=10,
)

