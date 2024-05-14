from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_key={
        'type': ARG_TYPE.STR,
        'description': 'API Key For Connecting to CoinBase API.',
        'required': True,
        'label': 'API Key',
        'secret': True
    },
    api_secret={
        'type': ARG_TYPE.PWD,
        'description': 'API Secret For Connecting to CoinBase API.',
        'required': True,
        'label': 'API Secret',
        'secret': True
    },
    api_passphrase={
        'type': ARG_TYPE.PWD,
        'description': 'API Passphrase.',
        'required': True,
        'label': 'API Passphrase',
        'secret': True
    },
)

connection_args_example = OrderedDict(
    api_key='public_key',
    api_secret='secret_key',
    api_passphrase='passphrase'
)
