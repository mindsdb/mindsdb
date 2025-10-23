from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


creation_args = OrderedDict(
    lemonade_api_key={
        'type': ARG_TYPE.STR,
        'description': 'API key for Lemonade server (can be any value, required but unused).',
        'required': False,
        'label': 'Lemonade API key',
        'secret': True
    },
    api_base={
        'type': ARG_TYPE.STR,
        'description': 'Base URL for Lemonade server.',
        'required': False,
        'label': 'API Base URL',
        'secret': False
    }
)
