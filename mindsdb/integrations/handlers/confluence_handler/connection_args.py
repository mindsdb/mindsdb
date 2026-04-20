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
        "description": "The username for basic authentication.",
        "label": "Username",
        "required": False,
    },
    password={
        "type": ARG_TYPE.STR,
        "description": "The password or API token for basic authentication.",
        "label": "Password",
        "required": False,
        "secret": True,
    },
    token={
        "type": ARG_TYPE.STR,
        "description": "The personal access token for bearer authentication.",
        "label": "Token",
        "required": False,
        "secret": True,
    },
    auth_method={
        "type": ARG_TYPE.STR,
        "description": "Authentication method to use. Supported values: 'basic', 'bearer'.",
        "label": "Auth Method",
        "required": False,
    },
    is_selfHosted={
        "type": ARG_TYPE.BOOL,
        "description": (
            "Set to True for Confluence Server / Data Center (on-premises), or False (default) for Confluence Cloud. "
            "When True, the handler uses the Confluence Server REST API v1. "
            "Note: 'whiteboards', 'databases', and 'tasks' tables are Cloud-only and will raise an error when self-hosted mode is enabled."
        ),
        "label": "Is Self Hosted",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    api_base="https://marios.atlassian.net/", token="your_personal_access_token", auth_method="bearer"
)
