from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "Gong API key for authentication.",
        "secret": True,
        "required": False,
        "label": "API Key",
    },
    # Basic Authentication with Access Key + Secret Key (Option 2)
    access_key={
        "type": ARG_TYPE.STR,
        "description": "Gong Access Key for basic authentication (if not using OAuth).",
        "secret": True,
        "required": False,
        "label": "Access Key",
    },
    secret_key={
        "type": ARG_TYPE.PWD,
        "description": "Gong Secret Key for basic authentication (if not using OAuth).",
        "secret": True,
        "required": False,
        "label": "Secret Key",
    },
    base_url={
        "type": ARG_TYPE.STR,
        "description": "Gong API base URL (optional, defaults to production).",
        "required": False,
        "label": "Base URL",
    },
)

connection_args_example = OrderedDict(api_key="your_gong_api_key_here", base_url="https://api.gong.io")
