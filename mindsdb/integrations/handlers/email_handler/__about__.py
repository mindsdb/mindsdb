# mindsdb/integrations/handlers/email_handler/__about__.py

from mindsdb.integrations.libs.const import HANDLER_TYPE

__title__ = "Email"
__package_name__ = "email_handler"
__version__ = "0.0.1"
__description__ = "MindsDB handler for retrieving emails through IMAP."
__author__ = "MindsDB Inc"
__github__ = "https://github.com/mindsdb/mindsdb"
__pypi__ = "https://pypi.org/project/mindsdb/"
__license__ = "GPL-3.0"
__copyright__ = "Copyright 2023- mindsdb"
__icon_path__ = "icon.svg"

# Suggestion 1: Use hasattr for safer assignment
HANDLER_TYPE = getattr(HANDLER_TYPE, "DATA", "data")

connection_args = {
    "email": {
        "type": "str",
        "description": "User's email address. Used to auto-detect settings if advanced options are not provided.",
        "required": True,
        "label": "Email Address",
    },
    "password": {
        "type": "str",
        "description": "Email account password or app-specific password.",
        "required": True,
        "secret": True,
        "label": "Password",
    },
    "host": {
        "type": "str",
        "description": "IMAP server host (e.g., imap.gmail.com or 127.0.0.1 for local proxies). Overrides automatic detection.",
        "required": False,
        "label": "IMAP Host",
        "group": "Advanced Settings",
    },
    "port": {
        "type": "int",
        "description": "IMAP server port. Overrides automatic detection (default: 993).",
        "required": False,
        "label": "IMAP Port",
        "group": "Advanced Settings",
    },
    "username": {
        "type": "str",
        "description": "Login username. If not provided, the email address is used.",
        "required": False,
        "label": "Username",
        "group": "Advanced Settings",
    },
    "use_ssl": {
        "type": "bool",
        "description": "Specifies whether to use an SSL-encrypted connection (default: True).",
        "required": False,
        "label": "Use SSL",
        "group": "Advanced Settings",
    },
}