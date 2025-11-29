from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    jira_url={
        "type": ARG_TYPE.STR,
        "description": "The URL of the Jira instance (e.g., https://your-domain.atlassian.net).",
        "required": True,
        "label": "Jira URL",
    },
    jira_username={
        "type": ARG_TYPE.STR,
        "description": "The username or email address used to authenticate with Jira.",
        "required": True,
        "label": "Username",
    },
    jira_password={
        "type": ARG_TYPE.PWD,
        "description": "The password used for authentication with Jira (alternative to API token).",
        "required": False,
        "label": "Password",
        "secret": True,
    },
    jira_personal_access_token={
        "type": ARG_TYPE.PWD,
        "description": "The personal access token used for authentication with Jira (alternative to API token).",
        "required": False,
        "label": "Personal Access Token",
        "secret": True,
    },
    jira_api_token={
        "type": ARG_TYPE.PWD,
        "description": "The API token used for authentication with Jira.",
        "required": True,
        "label": "API Token",
        "secret": True,
    },
    cloud={
        "type": ARG_TYPE.BOOL,
        "description": "Indicates whether to connect to Jira Cloud (True) or Jira Server (False). Default is True.",
        "required": False,
        "label": "Jira Cloud",
    }
)

connection_args_example = OrderedDict(
    jira_url="https://your-domain.atlassian.net",
    jira_username="user@example.com",
    jira_api_token="YOUR_API_TOKEN",
    cloud=True
)
