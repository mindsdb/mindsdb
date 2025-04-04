from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


creation_args = OrderedDict(
    azure_openai_api_key={
        'type': ARG_TYPE.STR,
        'description': 'Key for Azure OpenAI API.',
        'required': False,
        'label': 'OpenAI API key',
        'secret': True
    },
    azure_endpoint={
        'type': ARG_TYPE.STR,
        'description': 'Endpoint for Azure OpenAI API.',
        'required': False,
        'label': 'Azure OpenAI API endpoint',
    },
    azure_openai_api_version={
        'type': ARG_TYPE.STR,
        'description': 'Version for Azure OpenAI API.',
        'required': False,
        'label': 'Azure OpenAI API version',
    }
)
