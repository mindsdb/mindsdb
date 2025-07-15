from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "Raindrop.io API access token. You can get this from https://app.raindrop.io/settings/integrations",
        "required": True,
        "label": "API Key",
        "secret": True
    },
)

connection_args_example = OrderedDict(
    api_key="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeee"
)
