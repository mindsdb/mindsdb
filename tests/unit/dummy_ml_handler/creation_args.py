from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


creation_args = OrderedDict(
    api_key={
        'type': ARG_TYPE.STR,
        'description': 'Key',
        'required': False,
        'label': 'API key',
        'secret': True
    }
)
