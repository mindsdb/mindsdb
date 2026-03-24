from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


creation_args = OrderedDict(
    novita_api_key={
        "type": ARG_TYPE.STR,
        "description": "API key for Novita AI.",
        "required": False,
        "label": "Novita API key",
        "secret": True,
    },
    api_base={
        "type": ARG_TYPE.STR,
        "description": "Base URL for Novita API. Defaults to https://api.novita.ai/openai/v1",
        "required": False,
        "label": "API base URL",
        "secret": False,
    },
)
