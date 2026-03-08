from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    repository={
        "type": ARG_TYPE.STR,
        "description": "GitHub repository name (owner/repo). Optional if specified per query via WHERE clause.",
        "required": False,
        "label": "Repository",
    },
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "Optional GitHub API key to use for authentication.",
        "required": False,
        "label": "Api key",
        "secret": True
    },
    client_id={
        "type": ARG_TYPE.STR,
        "description": "GitHub App OAuth client ID. Used with client_secret for token refresh.",
        "required": False,
        "label": "Client ID",
    },
    client_secret={
        "type": ARG_TYPE.PWD,
        "description": "GitHub App OAuth client secret. Used with client_id for token refresh.",
        "required": False,
        "label": "Client Secret",
        "secret": True,
    },
    access_token={
        "type": ARG_TYPE.PWD,
        "description": "OAuth access token from GitHub App authorization flow.",
        "required": False,
        "label": "Access Token",
        "secret": True,
    },
    refresh_token={
        "type": ARG_TYPE.PWD,
        "description": "OAuth refresh token for automatic token renewal.",
        "required": False,
        "label": "Refresh Token",
        "secret": True,
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
