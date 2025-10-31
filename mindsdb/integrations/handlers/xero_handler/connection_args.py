from collections import OrderedDict
from mindsdb.integrations.handlers.xero_handler.__about__ import __version__ as version
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    # OAuth2 Code Flow Parameters
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
        'description': 'OAuth2 Redirect URI (must match your Xero app configuration). Required for code flow.',
        'label': 'Redirect URI',
        'required': False,
    },
    code={
        'type': ARG_TYPE.STR,
        'description': 'Authorization code obtained from OAuth flow (code flow only)',
        'label': 'Authorization Code',
        'required': False,
        'secret': True,
    },

    # Token Injection Parameters (for backend integration)
    access_token={
        'type': ARG_TYPE.STR,
        'description': 'Xero access token (for token injection from backend systems)',
        'label': 'Access Token',
        'required': False,
        'secret': True,
    },
    refresh_token={
        'type': ARG_TYPE.STR,
        'description': 'Xero refresh token (for automatic token refresh)',
        'label': 'Refresh Token',
        'required': False,
        'secret': True,
    },
    expires_at={
        'type': ARG_TYPE.STR,
        'description': 'Access token expiration time (ISO 8601 format or Unix timestamp)',
        'label': 'Token Expires At',
        'required': False,
    },

    # Organization/Tenant
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
