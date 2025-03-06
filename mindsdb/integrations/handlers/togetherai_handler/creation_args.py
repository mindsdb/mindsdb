from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


creation_args = OrderedDict(
    togetherai_api_key={
        "type": ARG_TYPE.STR,
        "description": "Key for TogetherAI API.",
        "required": False,
        "label": "TogetherAI API key",
        "secret": True,
    }
)
