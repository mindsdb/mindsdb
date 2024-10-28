from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    storage_account_name={
        'type': ARG_TYPE.STR,
        'description': 'The name of your storage service account',
        'required': True,
        'label': 'Storage Account Name'
    },
    account_access_key={
        'type': ARG_TYPE.STR,
        'description': 'Account Access Key',
        'required': True,
        'label': 'Account Access Key',
        'secret': True
    },
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
    },

)

connection_args_example = OrderedDict(
    storage_account_name='',
    account_access_key='',
    container_name='',
    connection_string=''
)
