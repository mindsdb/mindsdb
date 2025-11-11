from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    shop_url={
        'type': ARG_TYPE.STR,
        'description': 'Shop url',
        'required': True,
        'label': 'Shop url',
    },
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'Client ID of the app',
        'required': True,
        'label': 'client_id',
    },
    client_secret={
        'type': ARG_TYPE.PWD,
        'description': 'Secret of the app',
        'required': True,
        'label': 'Database',
        'secret': True,
    },
)

connection_args_example = OrderedDict(
    shop_url='shop-123456.myshopify.com',
    client_id='secret',
    client_secret='shpss_secret',
)
