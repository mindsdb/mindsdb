from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    credentials={
        'type': ARG_TYPE.PATH,
        'description': 'The path to the credentials file. If not specified, the default credentials are used.'
    }
)

connection_args_example = OrderedDict(
    credentials='/path/to/credentials.json'
)
