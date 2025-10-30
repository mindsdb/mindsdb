from collections import OrderedDict
from mindsdb.integrations.handlers.xero_handler.__about__ import __version__ as version
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'Xero OAuth2 Client ID',
        'label': 'OAuth Client ID',
        'required': True,
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'Xero OAuth2 Client Secret',
        'label': 'OAuth Client Secret',
        'required': True,
        'secret': True,
    },
    redirect_uri={
        'type': ARG_TYPE.STR,
        'description': 'OAuth2 Redirect URI (must match your Xero app configuration)',
        'label': 'Redirect URI',
        'required': True,
    },
    code={
        'type': ARG_TYPE.STR,
        'description': 'Authorization code obtained from OAuth flow',
        'label': 'Authorization Code',
        'required': False,
        'secret': True,
    },
    tenant_id={
        'type': ARG_TYPE.STR,
        'description': 'Xero Tenant ID (Organization ID). If not provided, the first accessible tenant will be used.',
        'label': 'Tenant ID',
        'required': False,
    },
)

connection_args_strict = OrderedDict(
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'Xero OAuth2 Client ID',
        'label': 'OAuth Client ID',
        'required': True,
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'Xero OAuth2 Client Secret',
        'label': 'OAuth Client Secret',
        'required': True,
        'secret': True,
    },
    redirect_uri={
        'type': ARG_TYPE.STR,
        'description': 'OAuth2 Redirect URI',
        'label': 'Redirect URI',
        'required': True,
    },
)
