from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


creation_args = OrderedDict(
    openai_api_key={
        'type': ARG_TYPE.STR,
        'description': 'Key for OpenAI API.',
        'required': False,
        'label': 'OpenAI API key',
        'secret': True
    }
)
