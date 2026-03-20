from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_base={
        "type": ARG_TYPE.URL,
        "description": "The base URL of the Confluence instance/server.",
        "label": "Base URL",
        "required": True,
    },
    username={
        "type": ARG_TYPE.STR,
        "description": "The username for the Confluence account. Not required when using api_token.",
        "label": "Username",
        "required": False,
    },
    password={
        "type": ARG_TYPE.STR,
        "description": "The API token for the Confluence account.",
        "label": "Password",
        "required": False,
        "secret": True,
    },
    api_token={
        "type": ARG_TYPE.STR,
        "description": "Personal Access Token for Bearer auth. When provided, username and password are not required.",
        "label": "API Token (PAT)",
        "required": False,
        "secret": True,
    },
)

connection_args_example = OrderedDict(
    api_base="https://marios.atlassian.net/", username="your_username", password="access_token"
)
