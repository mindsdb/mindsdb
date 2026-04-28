"""REST/passthrough connection arguments for the rest_api handler.

These fields configure how the handler talks HTTP to the upstream API
(base URL, allowed hosts, default headers, test path). Authentication
fields live in oauth_connection_args.py — keep them separate so the
passthrough plumbing stays independent of the auth strategy.
"""

from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


rest_connection_args = OrderedDict(
    base_url={
        "type": ARG_TYPE.STR,
        "description": "Base URL of the REST API (e.g. https://api.example.com)",
        "required": True,
        "label": "Base URL",
    },
    default_headers={
        "type": ARG_TYPE.DICT,
        "description": 'Static headers added to every request (e.g. {"Accept": "application/json"})',
        "required": False,
        "label": "Default Headers",
    },
    allowed_hosts={
        "type": ARG_TYPE.LIST,
        "description": 'Allowed hostnames for passthrough requests. Defaults to the base_url host. Use ["*"] to disable containment.',
        "required": False,
        "label": "Allowed Hosts",
    },
    test_path={
        "type": ARG_TYPE.STR,
        "description": "Path used by the /passthrough/test endpoint. Defaults to /",
        "required": False,
        "label": "Test Path",
    },
)
