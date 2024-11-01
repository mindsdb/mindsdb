from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


creation_args = OrderedDict(
    unify_api_key={
        'type': ARG_TYPE.STR,
        'description': 'Key for Unify API.',
        'required': False,
        'label': 'Unify API key',
        'secret': True
    }
)
