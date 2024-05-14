from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    app_id={
        "type": ARG_TYPE.PWD,
        "description": "App ID of symbl",
        "required": True,
        "label": "app_id",
    },
    app_secret={
        "type": ARG_TYPE.PWD,
        "description": "App Secret of symbl",
        "required": True,
        "label": "app_secret",
        "secret": True
    }
)

connection_args_example = OrderedDict(
    app_id="app_id",
    app_secret="app_secret"
)
