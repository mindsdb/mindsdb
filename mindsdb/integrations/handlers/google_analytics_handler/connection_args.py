from collections import OrderedDict
from mindsdb.integrations.handlers.google_analytics_handler.__about__ import __version__ as version
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    property_id={
        'type': ARG_TYPE.STR,
        'description': 'The Google Analytics 4 property ID (e.g., "123456789")',
        'label': 'GA4 Property ID',
        'required': True,
    },
    # Service Account Authentication
    credentials_file={
        'type': ARG_TYPE.PATH,
        'description': 'Full path to Google Service Account JSON file OR OAuth client secrets JSON file',
        'label': 'Credentials File Path',
        'required': False,
        'secret': True,
    },
    credentials_json={
        'type': ARG_TYPE.DICT,
        'description': 'Google Service Account credentials as a JSON object',
        'label': 'Credentials JSON',
        'required': False,
        'secret': True,
    },
    # OAuth2 Authentication - Authorization Code Flow
    credentials_url={
        'type': ARG_TYPE.STR,
        'description': 'URL to OAuth client secrets JSON file (alternative to credentials_file for OAuth)',
        'label': 'OAuth Credentials URL',
        'required': False,
        'secret': True,
    },
    code={
        'type': ARG_TYPE.STR,
        'description': 'Authorization code obtained after user consent (for OAuth flow)',
        'label': 'Authorization Code',
        'required': False,
        'secret': True,
    },
    # OAuth2 Authentication - Direct Refresh Token Method
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'OAuth client ID (for direct OAuth with refresh token)',
        'label': 'OAuth Client ID',
        'required': False,
        'secret': False,
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'OAuth client secret (for direct OAuth with refresh token)',
        'label': 'OAuth Client Secret',
        'required': False,
        'secret': True,
    },
    refresh_token={
        'type': ARG_TYPE.STR,
        'description': 'OAuth refresh token (for direct OAuth authentication)',
        'label': 'OAuth Refresh Token',
        'required': False,
        'secret': True,
    },
    token_uri={
        'type': ARG_TYPE.STR,
        'description': 'OAuth token URI (optional, defaults to https://oauth2.googleapis.com/token)',
        'label': 'OAuth Token URI',
        'required': False,
        'secret': False,
    },
    # Optional Parameters
    scopes={
        'type': ARG_TYPE.STR,
        'description': 'Comma-separated list of OAuth scopes (optional, uses default GA scopes if not provided)',
        'label': 'OAuth Scopes',
        'required': False,
        'secret': False,
    },
)

connection_args_example = OrderedDict(
    property_id='123456789',
    credentials_file='/path/to/service_account.json'
)

# Additional examples for OAuth authentication
oauth_example_with_refresh_token = OrderedDict(
    property_id='123456789',
    client_id='your-client-id.apps.googleusercontent.com',
    client_secret='your-client-secret',
    refresh_token='your-refresh-token'
)

oauth_example_with_code = OrderedDict(
    property_id='123456789',
    credentials_file='/path/to/client_secrets.json',
    code='authorization-code-from-oauth-flow'
)
