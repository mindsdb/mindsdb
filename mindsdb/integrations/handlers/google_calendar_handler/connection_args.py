from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    credentials={
        'type': ARG_TYPE.PATH,
        'description': 'Service Account Keys',
        'label': 'Upload Service Account Keys',
    },
)
