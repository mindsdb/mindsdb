from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    credentials_url={
        'type': ARG_TYPE.STR,
        'description': 'URL to Service Account Keys',
        'label': 'URL to Service Account Keys',
    },
    credentials_file={
        'type': ARG_TYPE.STR,
        'description': 'Location of Service Account Keys',
        'label': 'path of Service Account Keys',
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
