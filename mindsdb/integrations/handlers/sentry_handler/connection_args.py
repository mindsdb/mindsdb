from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    auth_token={
        "type": ARG_TYPE.PWD,
        "description": "Sentry API token.",
        "required": True,
        "label": "Auth token",
        "secret": True,
    },
    organization_slug={
        "type": ARG_TYPE.STR,
        "description": "Sentry organization slug.",
        "required": True,
        "label": "Organization slug",
    },
    project_slug={
        "type": ARG_TYPE.STR,
        "description": "Sentry project slug.",
        "required": True,
        "label": "Project slug",
    },
    environment={
        "type": ARG_TYPE.STR,
        "description": "Sentry environment name.",
        "required": True,
        "label": "Environment",
    },
    base_url={
        "type": ARG_TYPE.URL,
        "description": "Optional Sentry base URL origin.",
        "required": False,
        "label": "Base URL",
    },
)

connection_args_example = OrderedDict(
    auth_token="sntrys_xxx",
    organization_slug="talentify",
    project_slug="mktplace",
    environment="production",
    base_url="https://sentry.io",
)
