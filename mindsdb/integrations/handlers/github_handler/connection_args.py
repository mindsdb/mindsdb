from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    repository={
        "type": ARG_TYPE.STR,
        "description": " GitHub repository name.",
        "required": True,
        "label": "Repository",
    },
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "Optional GitHub API key to use for authentication.",
        "required": False,
        "label": "Api key",
        "secret": True
    },
    github_url={
        "type": ARG_TYPE.STR,
        "description": "Optional GitHub URL to connect to a GitHub Enterprise instance.",
        "required": False,
        "label": "Github url",
    },
)

connection_args_example = OrderedDict(
    repository="mindsdb/mindsdb",
    api_key="ghp_z91InCQZWZAMlddOzFCX7xHJrf9Fai35HT7",
    github_url="https://github.com/mindsdb/mindsdb"
)
