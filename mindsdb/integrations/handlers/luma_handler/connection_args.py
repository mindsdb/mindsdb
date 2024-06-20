from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.STR,
        "description": "Luma API Key",
        "required": True,
        "label": "api_key",
        "secret": True
    }
)

connection_args_example = OrderedDict(
    api_key="api_key"
)
