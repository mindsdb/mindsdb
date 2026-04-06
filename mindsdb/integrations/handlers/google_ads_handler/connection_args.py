from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    customer_id={
        'type': ARG_TYPE.STR,
        'description': 'Google Ads customer ID (format: 123-456-7890 or 1234567890)',
        'label': 'Customer ID',
        'required': True,
    },
    developer_token={
        'type': ARG_TYPE.STR,
        'description': 'Google Ads API developer token (from Google Ads UI > Tools & Settings > API Center)',
        'label': 'Developer Token',
        'required': True,
        'secret': True,
    },
    login_customer_id={
        'type': ARG_TYPE.STR,
        'description': 'MCC (manager) account ID when accessing client accounts through a manager account',
        'label': 'Login Customer ID (MCC)',
        'required': False,
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
        'description': 'OAuth refresh token (must include the adwords scope)',
        'label': 'OAuth Refresh Token',
        'required': False,
        'secret': True,
    },
    token_uri={
        'type': ARG_TYPE.STR,
        'description': 'OAuth token URI (optional, defaults to https://oauth2.googleapis.com/token)',
        'label': 'OAuth Token URI',
        'required': False,
    },
    scopes={
        'type': ARG_TYPE.STR,
        'description': 'Comma-separated OAuth scopes (optional, defaults to https://www.googleapis.com/auth/adwords)',
        'label': 'OAuth Scopes',
        'required': False,
    },
)

connection_args_example = OrderedDict(
    customer_id='123-456-7890',
    developer_token='your-developer-token',
    client_id='your-client-id.apps.googleusercontent.com',
    client_secret='your-client-secret',
    refresh_token='your-refresh-token',
)
