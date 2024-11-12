from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.STR,
        "description": "API key",
        "required": True,
        "label": "api_key",
        "secret": True
    },
    sub_domain={
        "type": ARG_TYPE.STR,
        "description": "Sub-domain",
        "required": True,
        "label": "sub_domain",
        "secret": True
    },
    email={
        "type": ARG_TYPE.STR,
        "description": "Email ID",
        "required": True,
        "label": "email"
    }
)

connection_args_example = OrderedDict(
    api_key="api_key",
    sub_domain="sub_domain",
    email="email"
)
