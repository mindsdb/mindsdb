from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_key={
        'type': ARG_TYPE.PWD,
        'description': 'Gong API key for authentication.',
        'secret': True,
        'required': True,
        'label': 'API Key'
    },
    base_url={
        'type': ARG_TYPE.STR,
        'description': 'Gong API base URL (optional, defaults to production).',
        'required': False,
        'label': 'Base URL'
    }
)

connection_args_example = OrderedDict(
    api_key='your_gong_api_key_here',
    base_url='https://api.gong.io'
) 