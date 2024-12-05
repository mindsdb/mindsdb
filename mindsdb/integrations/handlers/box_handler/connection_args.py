from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    token={
        "type": ARG_TYPE.STR,
        "description": "Box Developer Token",
        "required": True,
        "label": "Box Developer Token",
    },
)

connection_args_example = OrderedDict(
    token="",
)
