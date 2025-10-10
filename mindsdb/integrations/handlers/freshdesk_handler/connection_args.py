from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.STR,
        "description": "Freshdesk API key",
        "required": True,
        "label": "api_key",
        "secret": True
    },
    domain={
        "type": ARG_TYPE.STR,
        "description": "Freshdesk domain (e.g., 'yourcompany.freshdesk.com')",
        "required": True,
        "label": "domain",
        "secret": False
    }
)

connection_args_example = OrderedDict(
    api_key="your_api_key_here",
    domain="yourcompany.freshdesk.com"
)
