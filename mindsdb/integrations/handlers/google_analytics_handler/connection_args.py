from collections import OrderedDict
from mindsdb.integrations.handlers.google_analytics_handler.__about__ import __version__ as version
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    property_id={
        'type': ARG_TYPE.STR,
        'description': 'The Google Analytics 4 property ID (e.g., "123456789")',
        'label': 'GA4 Property ID',
        'required': True,
    },
    credentials_file={
        'type': ARG_TYPE.PATH,
        'description': 'Full path to the Google Service Account credentials JSON file',
        'label': 'Credentials File Path',
        'required': False,
        'secret': True,
    },
    credentials_json={
        'type': ARG_TYPE.DICT,
        'description': 'Google Service Account credentials as a JSON object',
        'label': 'Credentials JSON',
        'required': False,
        'secret': True,
    },
)

connection_args_example = OrderedDict(
    property_id='123456789',
    credentials_file='/path/to/credentials.json'
)
