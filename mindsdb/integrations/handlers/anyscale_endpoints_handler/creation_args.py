from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


creation_args = OrderedDict(
    anyscale_endpoints_api_key={
        'type': ARG_TYPE.STR,
        'description': 'Key for anyscale endpoints.',
        'required': False,
        'label': 'Anyscale endpoints API key',
        'secret': True
    }
)
