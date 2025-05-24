from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    api_key = {
        'type': ARG_TYPE.PWD,
        'description': 'CoinMarketCap API key',
        'required': True,
        'label' : 'API Key',
        'secret': True
    },
    base_url = {
        'type': ARG_TYPE.STR,
        'description': 'CoinMarketCap API URL. Default is https://pro-api.coinmarketcap.com/v1/',
        'required': False,
        'label' : 'Base URL',
    }
)

connection_args_example = OrderedDict(
    api_key = '9d88d55d-42a9-4cef-85a8-5b9eab8335b6',
    base_url = 'https://pro-api.coinmarketcap.com/v1/'
)