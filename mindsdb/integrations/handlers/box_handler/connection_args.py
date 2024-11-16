from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'The client ID for the Box application.',
        'required': True,
        'label': 'Client ID'
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'The client secret for the Box application.',
        'secret': True,
        'required': True,
        'label': 'Client Secret'
    },
    access_token={
        'type': ARG_TYPE.STR,
        'description': 'The access token for the Box application.',
        'secret': True,
        'required': False,
        'label': 'Access Token'
    },
    refresh_token={
        'type': ARG_TYPE.STR,
        'description': 'The refresh token for the Box application.',
        'secret': True,
        'required': False,
        'label': 'Refresh Token'
    }
)

connection_args_example = OrderedDict(
    client_id='YOUR_CLIENT_ID',
    client_secret='YOUR_CLIENT_SECRET',
    access_token='YOUR_ACCESS_TOKEN',
    refresh_token='YOUR_REFRESH_TOKEN'
)
