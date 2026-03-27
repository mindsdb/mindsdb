from collections import OrderedDict

from mindsdb.integrations.handlers.linkedin_ads_handler.__about__ import __version__ as version
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    account_id={
        'type': ARG_TYPE.STR,
        'description': 'LinkedIn Ads account ID selected during OAuth setup.',
        'label': 'Ad Account ID',
        'required': True,
    },
    access_token={
        'type': ARG_TYPE.STR,
        'description': 'LinkedIn Ads OAuth access token.',
        'label': 'Access Token',
        'required': False,
        'secret': True,
    },
    refresh_token={
        'type': ARG_TYPE.STR,
        'description': 'LinkedIn Ads OAuth refresh token used for automatic token renewal.',
        'label': 'Refresh Token',
        'required': False,
        'secret': True,
    },
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'LinkedIn OAuth client ID. Required for refresh token exchanges.',
        'label': 'OAuth Client ID',
        'required': False,
        'secret': True,
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'LinkedIn OAuth client secret. Required for refresh token exchanges.',
        'label': 'OAuth Client Secret',
        'required': False,
        'secret': True,
    },
    api_version={
        'type': ARG_TYPE.STR,
        'description': 'LinkedIn Marketing API version header, for example 202602.',
        'label': 'API Version',
        'required': False,
        'secret': False,
    },
)

connection_args_example = OrderedDict(
    account_id='516413367',
    access_token='your_access_token_here',
    refresh_token='your_refresh_token_here',
    client_id='your_client_id_here',
    client_secret='your_client_secret_here',
    api_version='202602',
)
