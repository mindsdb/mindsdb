"""Authentication connection arguments for the rest_api handler.

These fields describe *how* the handler should authenticate to the upstream
API. The current strategies are static bearer tokens and OAuth2 client
credentials; both share this argument schema. The handler — not the runtime
caller — is responsible for resolving these into an Authorization header.

Keep this module focused on schema only: do not import the OAuth2 token
provider here, do not perform any HTTP, and do not change passthrough
forwarding behavior. This step only defines the args.
"""

from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


oauth_connection_args = OrderedDict(
    auth_type={
        "type": ARG_TYPE.STR,
        "description": (
            "Authentication strategy. 'bearer' uses a static bearer_token; "
            "'oauth_client_credentials' fetches a token via the OAuth2 client "
            "credentials grant. Defaults to 'bearer' for backward compatibility."
        ),
        "required": False,
        "label": "Auth Type",
    },
    bearer_token={
        "type": ARG_TYPE.PWD,
        "description": "Bearer token injected as Authorization: Bearer <token>. Used when auth_type is 'bearer'.",
        "required": False,
        "label": "Bearer Token",
        "secret": True,
    },
    token_url={
        "type": ARG_TYPE.STR,
        "description": "OAuth2 token endpoint URL. Used when auth_type is 'oauth_client_credentials'.",
        "required": False,
        "label": "OAuth Token URL",
    },
    client_id={
        "type": ARG_TYPE.STR,
        "description": "OAuth2 client identifier. Used when auth_type is 'oauth_client_credentials'.",
        "required": False,
        "label": "OAuth Client ID",
    },
    client_secret={
        "type": ARG_TYPE.PWD,
        "description": "OAuth2 client secret. Used when auth_type is 'oauth_client_credentials'.",
        "required": False,
        "label": "OAuth Client Secret",
        "secret": True,
    },
    scope={
        "type": ARG_TYPE.STR,
        "description": "Optional OAuth2 scope string (space-separated) or list of scopes.",
        "required": False,
        "label": "OAuth Scope",
    },
    audience={
        "type": ARG_TYPE.STR,
        "description": "Optional OAuth2 audience parameter (Auth0/Cognito-style extension; not part of RFC 6749).",
        "required": False,
        "label": "OAuth Audience",
    },
    token_auth_method={
        "type": ARG_TYPE.STR,
        "description": (
            "How client credentials are sent to the token endpoint: "
            "'client_secret_post' (default) or 'client_secret_basic'."
        ),
        "required": False,
        "label": "Token Auth Method",
    },
)
