from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    url={
        "type": ARG_TYPE.URL,
        "description": "Confluence URL",
        "label": "url",
        "required": True
    },
    username={
        "type": ARG_TYPE.STR,
        "description": "Confluence username",
        "label": "username",
        "required": True
    },
    password={
        "type": ARG_TYPE.STR,
        "description": "Password",
        "label": "password",
        "required": True,
        "secret": True
    }
)

connection_args_example = OrderedDict(
    url="https://marios.atlassian.net/",
    username="your_username",
    password="access_token"
)
