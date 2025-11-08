from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    url={
        'type': ARG_TYPE.STR,
        'description': 'Default URL for API endpoint. If specified, queries do not need to include a URL in the WHERE clause.',
        'required': False,
        'label': 'Default URL',
    },
    headers={
        'type': ARG_TYPE.DICT,
        'description': 'Default HTTP headers as a dictionary. Used for authentication (e.g., API keys, Bearer tokens).',
        'required': False,
        'label': 'Default Headers',
    },
    timeout={
        'type': ARG_TYPE.INT,
        'description': 'Default request timeout in seconds. Default is 30 seconds.',
        'required': False,
        'label': 'Default Timeout',
    },
    max_content_size={
        'type': ARG_TYPE.INT,
        'description': 'Maximum response size in MB. Prevents downloading extremely large files. Default is 100 MB.',
        'required': False,
        'label': 'Max Content Size (MB)',
    },
)


connection_args_example = OrderedDict(
    url='https://api.talentify.io/linkedin-slots/feed',
    headers={
        'Authorization': 'Bearer YOUR_TOKEN_HERE',
        'X-API-Key': 'your-api-key',
    },
    timeout=60,
    max_content_size=50,  # 50 MB limit
)
