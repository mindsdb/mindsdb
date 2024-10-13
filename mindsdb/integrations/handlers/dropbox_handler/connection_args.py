from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    repository={
        "type": ARG_TYPE.STR,
        "description": " Dropbox repository name.",
        "required": True,
        "label": "Repository",
    },
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "Optional Dropbox API key to use for authentication.",
        "required": False,
        "label": "Api key",
        "secret": True,
    },
    dropbox_url={
        "type": ARG_TYPE.STR,
        "description": "Optional Dropbox URL to connect to a Dropbox Enterprise instance.",
        "required": False,
        "label": "Dropbox url",
    },
)

connection_args_example = OrderedDict(
    repository="mindsdb/mindsdb",
    api_key="ghp_xxx",
    github_url="https://github.com/mindsdb/mindsdb",
)
