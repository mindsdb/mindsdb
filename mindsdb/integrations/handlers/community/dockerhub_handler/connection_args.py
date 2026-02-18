from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    username={
        "type": ARG_TYPE.STR,
        "description": "DockerHub username",
        "required": True,
        "label": "username",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "DockerHub password",
        "required": True,
        "label": "Api key",
        'secret': True
    }
)

connection_args_example = OrderedDict(
    username="username",
    password="password"
)
