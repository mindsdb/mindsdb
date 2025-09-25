from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    credentials_url={
        'type': ARG_TYPE.STR,
        'description': 'URL to Service Account Keys',
        'label': 'URL to Service Account Keys',
    },
    credentials_file={
        'type': ARG_TYPE.STR,
        'description': 'Location of Service Account Keys',
        'label': 'path of Service Account Keys',
    },
    credentials={
        'type': ARG_TYPE.PATH,
        'description': 'Service Account Keys',
        'label': 'Upload Service Account Keys',
    },
    code={
        'type': ARG_TYPE.STR,
        'description': 'Code After Authorisation',
        'label': 'Code After Authorisation',
    },
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'OAuth client ID for the Google project',
        'label': 'OAuth Client ID',
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'OAuth client secret for the Google project',
        'label': 'OAuth Client Secret',
    },
    refresh_token={
        'type': ARG_TYPE.STR,
        'description': 'User refresh token obtained during OAuth consent',
        'label': 'Refresh Token',
    },
    token_uri={
        'type': ARG_TYPE.STR,
        'description': 'Optional override for the OAuth token URI',
        'label': 'Token URI',
    },
    scopes={
        'type': ARG_TYPE.STR,
        'description': 'Comma separated OAuth scopes to request',
        'label': 'OAuth Scopes',
    },
)
