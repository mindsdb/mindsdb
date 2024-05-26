from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': "The host name or IP address of the Apache Ignite cluster's node.",
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': "The TCP/IP port of the Apache Ignite cluster's node. Must be an integer.",
        'required': True,
        'label': 'Port'
    },
    username={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Apache Ignite cluster. This parameter is optional. Default: None.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Apache Ignite cluster. This parameter is optional. Default: None.',
        'required': True,
        'label': 'Password',
        'secret': True
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': 'Schema to use for the connection to the Apache Ignite cluster. This parameter is optional. Default: PUBLIC.',
        'required': True,
        'label': 'Schema'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port='10800',
    username='root',
    password='password',
    schema='schema'
)
