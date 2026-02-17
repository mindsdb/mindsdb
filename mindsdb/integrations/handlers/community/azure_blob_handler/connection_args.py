from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    container_name={
        'type': ARG_TYPE.STR,
        'description': 'The name of your storage service account Container Name',
        'required': True,
        'label': 'Container Name'
    },
    connection_string={
        'type': ARG_TYPE.STR,
        'description': 'Connection String',
        'required': True,
        'label': 'Connection String',
        'secret': True
    }
)

connection_args_example = OrderedDict(
    container_name='',
    connection_string=''
)
