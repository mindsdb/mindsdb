from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    youtube_api_token={
        'type': ARG_TYPE.STR,
        'description': 'Youtube API Token',
        'label': 'Youtube API Token',
    },
    credentials_url={
        'type': ARG_TYPE.STR,
        'description': 'URL to Service Account Keys',
        'label': 'URL to Service Account Keys',
    },
    credentials_file={
        'type': ARG_TYPE.STR,
        'description': 'Location of Service Account Keys',
        'label': 'Path to Service Account Keys',
    },
    credentials={
        'type': ARG_TYPE.PATH,
        'description': 'Service Account Keys',
        'label': 'Upload Service Account Keys',
    },
    code={
        'type': ARG_TYPE.STR,
        'description': 'Code After Authorisation',
        'label': 'Code After Authorisation',
    },
)
